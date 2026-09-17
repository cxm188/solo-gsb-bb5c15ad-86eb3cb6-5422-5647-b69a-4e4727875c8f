"""FastAPI 入口：多桶滚动配额判定 API。

核心保证：
- 同一请求的所有桶在单个 BEGIN IMMEDIATE 事务内"检查 + 扣减"，
  任一超限则整体不扣减（原子性）；
- BEGIN IMMEDIATE 使写事务串行化，并发请求不会突破限额；
- request_id 首次出现时固化载荷与判定，重复提交原样返回，
  载荷变化返回 409；
- 首次判定（放行/拒绝）与首次成功释放在各自原事务内追加一条
  可校验哈希审计链记录（见 app/audit.py），幂等重放不重复记录。
"""
from __future__ import annotations

import json
import logging
import math
import os
import sqlite3
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse

from . import audit, config
from .db import _rollback_quietly, connect, init_db, seed_buckets
from .quota import earliest_retry_at, merge_items, payload_hash
from .schemas import BucketConfig, ConsumeRequest, ReleaseRequest, SimulationRequest
from . import simulation as sim_engine

logger = logging.getLogger("quota")


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    if config.BUCKETS_CONFIG and os.path.exists(config.BUCKETS_CONFIG):
        seeded = seed_buckets(config.BUCKETS_CONFIG)
        logger.info("seeded buckets from %s: %s", config.BUCKETS_CONFIG, seeded)
    yield


app = FastAPI(title="Rolling Quota API", version="1.0.0", lifespan=lifespan)


# ---------------------------------------------------------------- 配额判定

@app.post("/v1/quota/consume")
def consume(req: ConsumeRequest):
    """检查并扣减一组桶；任一超限则全部不扣减。

    - 200：全部放行，已扣减；
    - 429：至少一个桶超限，返回受限桶与最早重试时间，未做任何扣减；
    - 409：request_id 已存在但载荷不同；
    - 404：存在未配置的桶。
    """
    now = time.time()
    items = merge_items([i.model_dump() for i in req.buckets])
    phash = payload_hash(items)
    payload_json = json.dumps(
        {"request_id": req.request_id, "buckets": items}, sort_keys=True
    )

    replayed = False
    conn = connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        try:
            row = conn.execute(
                "SELECT payload_hash, decision FROM requests WHERE request_id = ?",
                (req.request_id,),
            ).fetchone()

            if row is not None:
                # 幂等：同 request_id 直接返回已固化的判定，不再扣减
                if row["payload_hash"] != phash:
                    raise HTTPException(
                        status_code=409,
                        detail=(
                            f"request_id '{req.request_id}' 已被不同载荷使用；"
                            "幂等键不可复用，请更换 request_id"
                        ),
                    )
                decision = json.loads(row["decision"])
                replayed = True
                conn.execute("COMMIT")
            else:
                decision = _evaluate_and_consume(conn, req.request_id, items, now)
                conn.execute(
                    "INSERT INTO requests "
                    "(request_id, payload_hash, payload, decision, allowed, created_at) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        req.request_id,
                        phash,
                        payload_json,
                        json.dumps(decision),
                        1 if decision["allowed"] else 0,
                        now,
                    ),
                )
                # 首次判定（放行或拒绝）在同一事务内追加审计记录；
                # 幂等重放走上面的已存在分支，不会重复记录
                audit.append_event(
                    conn,
                    op=audit.OP_CONSUME,
                    business_id=req.request_id,
                    result=(
                        audit.RESULT_ALLOWED
                        if decision["allowed"]
                        else audit.RESULT_DENIED
                    ),
                    occurred_at=now,
                )
                conn.execute("COMMIT")
        except Exception:
            _rollback_quietly(conn)
            raise
    finally:
        conn.close()

    return _decision_response(decision, replayed)


def _evaluate_and_consume(
    conn: sqlite3.Connection, request_id: str, items: list[dict], now: float
) -> dict:
    """在事务内检查全部桶；全部通过才写入扣减事件。"""
    limited: list[dict] = []

    for item in items:
        bucket = conn.execute(
            "SELECT limit_amount, window_seconds FROM buckets WHERE key = ?",
            (item["key"],),
        ).fetchone()
        if bucket is None:
            raise HTTPException(
                status_code=404,
                detail=f"桶 '{item['key']}' 未配置，请先通过 PUT /v1/buckets/{item['key']} 创建",
            )

        limit = bucket["limit_amount"]
        window = bucket["window_seconds"]
        events = [
            (r["created_at"], r["amount"])
            for r in conn.execute(
                "SELECT created_at, amount FROM usage_events "
                "WHERE bucket_key = ? AND created_at > ? ORDER BY created_at",
                (item["key"], now - window),
            )
        ]
        used = sum(a for _, a in events)

        if used + item["amount"] > limit:
            retry_at = earliest_retry_at(events, limit, window, item["amount"], now)
            limited.append(
                {
                    "key": item["key"],
                    "limit": limit,
                    "window_seconds": window,
                    "used": used,
                    "requested": item["amount"],
                    "retry_at": retry_at,
                    "retry_after_seconds": (
                        None if retry_at is None else max(0.0, retry_at - now)
                    ),
                }
            )

    if limited:
        # 任一超限：全部不扣减；整体最早重试时间取各受限桶的最大值
        retry_times = [b["retry_at"] for b in limited]
        overall_retry_at = (
            max(retry_times) if all(t is not None for t in retry_times) else None
        )
        return {
            "request_id": request_id,
            "allowed": False,
            "limited_buckets": limited,
            "retry_at": overall_retry_at,
            "retry_after_seconds": (
                None if overall_retry_at is None else max(0.0, overall_retry_at - now)
            ),
            "decided_at": now,
        }

    # 全部通过：同事务写入所有桶的扣减事件
    for item in items:
        conn.execute(
            "INSERT INTO usage_events (bucket_key, request_id, amount, created_at) "
            "VALUES (?, ?, ?, ?)",
            (item["key"], request_id, item["amount"], now),
        )
    return {
        "request_id": request_id,
        "allowed": True,
        "granted": items,
        "decided_at": now,
    }


def _decision_response(decision: dict, replayed: bool) -> JSONResponse:
    headers = {}
    if replayed:
        headers["X-Idempotent-Replay"] = "true"
    if decision["allowed"]:
        return JSONResponse(status_code=200, content=decision, headers=headers)
    if decision.get("retry_after_seconds") is not None:
        headers["Retry-After"] = str(math.ceil(decision["retry_after_seconds"]))
    return JSONResponse(status_code=429, content=decision, headers=headers)


# ---------------------------------------------------------------- 配额释放

@app.post("/v1/quota/releases")
def release(req: ReleaseRequest):
    """撤销一次已放行消费对所有桶的用量贡献。

    首次释放在单个 BEGIN IMMEDIATE 事务内完成"校验 → 删除该请求全部用量
    事件 → 标记消费已释放 → 固化释放结果"，任一环节失败整体回滚，
    不会出现部分桶已恢复。

    - 200：释放成功（或同 release_id + 同 request_id 重放，原样返回并带
      X-Idempotent-Replay: true）；
    - 404：原消费 request_id 不存在；
    - 409：原判定为拒绝 / 已被其他 release_id 释放 / release_id 已指向
      其他 request_id / 并发竞争落败。
    """
    now = time.time()

    conn = connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        try:
            existing = conn.execute(
                "SELECT request_id, result FROM releases WHERE release_id = ?",
                (req.release_id,),
            ).fetchone()
            if existing is not None:
                # 幂等：同 release_id 必须指向同一目标，原样返回固化结果
                if existing["request_id"] != req.request_id:
                    raise HTTPException(
                        status_code=409,
                        detail=(
                            f"release_id '{req.release_id}' 已用于释放 "
                            f"request_id '{existing['request_id']}'，不可改指其他消费请求"
                        ),
                    )
                conn.execute("COMMIT")
                result = json.loads(existing["result"])
                replayed = True
            else:
                result = _release_consumption(conn, req.release_id, req.request_id, now)
                conn.execute(
                    "INSERT INTO releases (release_id, request_id, result, created_at) "
                    "VALUES (?, ?, ?, ?)",
                    (
                        req.release_id,
                        req.request_id,
                        json.dumps(result),
                        now,
                    ),
                )
                # 仅首次成功释放在同一事务内追加审计记录；
                # 失败（404/409）整体回滚、重放走已存在分支，均不记录
                audit.append_event(
                    conn,
                    op=audit.OP_RELEASE,
                    business_id=req.release_id,
                    result=audit.RESULT_RELEASED,
                    occurred_at=now,
                )
                conn.execute("COMMIT")
                replayed = False
        except Exception:
            _rollback_quietly(conn)
            raise
    finally:
        conn.close()

    return JSONResponse(
        status_code=200,
        content=result,
        headers={"X-Idempotent-Replay": "true"} if replayed else {},
    )


def _release_consumption(
    conn: sqlite3.Connection, release_id: str, request_id: str, now: float
) -> dict:
    """在事务内撤销一个消费请求对所有桶的用量贡献并记录占用关系。"""
    row = conn.execute(
        "SELECT allowed, payload, release_id FROM requests WHERE request_id = ?",
        (request_id,),
    ).fetchone()
    if row is None:
        raise HTTPException(
            status_code=404,
            detail=f"消费请求 '{request_id}' 不存在，无法释放",
        )
    if not row["allowed"]:
        # 原判定为拒绝：从未产生任何扣减，无可释放内容
        raise HTTPException(
            status_code=409,
            detail=f"消费请求 '{request_id}' 原判定为拒绝（429），未发生扣减，不可释放",
        )
    if row["release_id"] is not None:
        # 同一消费只能由一个 release_id 成功释放；并发竞争中的其余请求亦落到此
        raise HTTPException(
            status_code=409,
            detail=(
                f"消费请求 '{request_id}' 已被 release_id '{row['release_id']}' 释放；"
                "同一消费请求只能释放一次"
            ),
        )

    payload = json.loads(row["payload"])
    # 删除该请求对所有桶的用量贡献（与首次扣减写入的事件一一对应）
    deleted = conn.execute(
        "DELETE FROM usage_events WHERE request_id = ?",
        (request_id,),
    ).rowcount
    # 条件 UPDATE 兜底并发竞争：仅当尚未被其他释放单占用时成功
    cur = conn.execute(
        "UPDATE requests SET release_id = ? WHERE request_id = ? AND release_id IS NULL",
        (release_id, request_id),
    )
    if cur.rowcount != 1:  # pragma: no cover - 理论上前置检查已覆盖，防御性保留
        raise HTTPException(
            status_code=409,
            detail=f"消费请求 '{request_id}' 已被其他释放单抢先释放",
        )

    return {
        "release_id": release_id,
        "request_id": request_id,
        "released": True,
        "released_buckets": payload["buckets"],
        "events_reverted": deleted,
        "released_at": now,
    }


# ---------------------------------------------------------------- 配额配置模拟

@app.post("/v1/quota/simulations")
def run_simulation(req: SimulationRequest):
    """在隔离内存中回放历史消费，评估候选桶配置的判定差异。

    全程只读数据库，不写任何业务数据、不追加审计记录；release 不回放。

    - 200：返回区间内原始判定 → 模拟判定的转移计数、每桶模拟峰值与
      前 100 条判定翻转明细；
    - 422：候选桶未配置 / from >= to / 区间跨度超过 7 天 / 候选桶重复。
    """
    start = float(req.from_)
    end = float(req.to)
    if start >= end:
        raise HTTPException(
            status_code=422,
            detail=f"非法区间：from({req.from_}) 必须严格小于 to({req.to})",
        )
    if end - start > sim_engine.MAX_RANGE_SECONDS:
        raise HTTPException(
            status_code=422,
            detail=(
                f"区间跨度 {int(end - start)} 秒超过上限 "
                f"{sim_engine.MAX_RANGE_SECONDS} 秒（7 天）"
            ),
        )

    candidate: dict[str, tuple[int, float]] = {}
    for b in req.buckets:
        if b.key in candidate:
            raise HTTPException(
                status_code=422,
                detail=f"候选桶 '{b.key}' 重复，每个桶至多给出一份候选配置",
            )
        candidate[b.key] = (b.limit, b.window_seconds)

    conn = connect()
    try:
        # 模拟配置：候选覆盖当前配置，未覆盖桶沿用当前配置
        rows_cfg = conn.execute(
            "SELECT key, limit_amount, window_seconds FROM buckets"
        ).fetchall()
        configs: dict[str, tuple[int, float]] = {
            r["key"]: (r["limit_amount"], r["window_seconds"]) for r in rows_cfg
        }
        unknown = [k for k in candidate if k not in configs]
        if unknown:
            raise HTTPException(
                status_code=422,
                detail=(
                    f"未知桶 {sorted(unknown)}：候选桶必须已存在，"
                    "请先通过 PUT /v1/buckets/{key} 创建"
                ),
            )
        configs.update(candidate)

        warm_start = sim_engine.warm_start_for(configs, start)
        # 仅回放消费（requests 固化的首次载荷）；release_id 非空的消费已被
        # 释放，模拟时同样跳过。按 (created_at, request_id) 稳定排序。
        rows = conn.execute(
            """
            SELECT request_id, created_at, payload, allowed, release_id
              FROM requests
             WHERE created_at >= ? AND created_at < ?
             ORDER BY created_at ASC, request_id ASC
            """,
            (warm_start, end),
        ).fetchall()
    finally:
        conn.close()

    return sim_engine.simulate(rows, configs=configs, start=start, end=end)


# ---------------------------------------------------------------- 哈希审计链

@app.get("/v1/audit")
def get_audit(
    after_seq: int = Query(0, ge=0, description="仅返回 seq 大于该值的记录"),
    limit: int = Query(100, ge=1, le=1000, description="单次返回的最大记录数"),
):
    """按 seq 升序返回审计记录及当前链头。"""
    conn = connect()
    try:
        records, head_seq, head_hash = audit.list_events(conn, after_seq, limit)
    finally:
        conn.close()
    return {
        "records": records,
        "head": {"seq": head_seq, "hash": head_hash},
        "next_after_seq": records[-1]["seq"] if records else after_seq,
    }


@app.get("/v1/audit/verify")
def verify_audit():
    """全链重算并返回校验结果。

    - valid：链是否完整无误；
    - checked_count：校验的记录条数；
    - head_hash：当前链头哈希（空链为 64 个 0）；
    - first_invalid_seq：首个失配记录的 seq，无失配时为 null。
    """
    conn = connect()
    try:
        return audit.verify_chain(conn)
    finally:
        conn.close()


# ---------------------------------------------------------------- 桶管理

@app.put("/v1/buckets/{key}", status_code=200)
def upsert_bucket(key: str, cfg: BucketConfig):
    """创建或更新桶的限额与窗口。"""
    conn = connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            """
            INSERT INTO buckets (key, limit_amount, window_seconds)
            VALUES (?, ?, ?)
            ON CONFLICT (key) DO UPDATE SET
                limit_amount   = excluded.limit_amount,
                window_seconds = excluded.window_seconds
            """,
            (key, cfg.limit, cfg.window_seconds),
        )
        conn.execute("COMMIT")
    except Exception:
        _rollback_quietly(conn)
        raise
    finally:
        conn.close()
    return {"key": key, "limit": cfg.limit, "window_seconds": cfg.window_seconds}


@app.get("/v1/buckets")
def list_buckets():
    conn = connect()
    try:
        rows = conn.execute(
            "SELECT key, limit_amount, window_seconds FROM buckets ORDER BY key"
        ).fetchall()
    finally:
        conn.close()
    return {
        "buckets": [
            {"key": r["key"], "limit": r["limit_amount"], "window_seconds": r["window_seconds"]}
            for r in rows
        ]
    }


@app.get("/v1/buckets/{key}/usage")
def bucket_usage(key: str):
    """查询桶当前滚动窗口用量。"""
    now = time.time()
    conn = connect()
    try:
        bucket = conn.execute(
            "SELECT limit_amount, window_seconds FROM buckets WHERE key = ?", (key,)
        ).fetchone()
        if bucket is None:
            raise HTTPException(status_code=404, detail=f"桶 '{key}' 未配置")
        window = bucket["window_seconds"]
        events = conn.execute(
            "SELECT created_at, amount FROM usage_events "
            "WHERE bucket_key = ? AND created_at > ? ORDER BY created_at",
            (key, now - window),
        ).fetchall()
    finally:
        conn.close()

    used = sum(r["amount"] for r in events)
    oldest = events[0]["created_at"] if events else None
    return {
        "key": key,
        "limit": bucket["limit_amount"],
        "window_seconds": window,
        "used": used,
        "remaining": max(0, bucket["limit_amount"] - used),
        "window_start": now - window,
        "as_of": now,
        "oldest_event_expires_at": None if oldest is None else oldest + window,
    }


@app.get("/healthz")
def healthz():
    return {"status": "ok"}
