"""配额配置模拟回放（纯内存，绝不写库、不追加审计）。

在一个隔离的内存状态机中，按 ``(created_at, request_id)`` 的稳定顺序
回放历史首次消费载荷，比较两套世界的判定差异：

- **原始世界（original）**：每条消费首次判定时固化在 ``requests.decision``
  里的 ``allowed`` 结果；
- **模拟世界（simulated）**：以候选桶配置（未覆盖桶沿用当前配置）重算
  滚动窗口过期与多桶 all-or-nothing 判定。

关键规则：

- **预热期**：从 ``from`` 向前取"所有已配置桶候选/当前窗口"的最大值作为
  预热长度。预热记录（``warm_start <= created_at < from``）只用来建立
  模拟世界的滚动窗口状态，不计任何统计；原始世界直接使用固化判定，
  无需重放；
- **只回放消费**：``requests`` 中固化的载荷即首次消费内容；已被 release
  的消费在真实世界中事件已删除，模拟时必须同样忽略——预热期内的已释放
  消费自然查不到仍生效的事件，区间内的已释放消费回放会虚增用量，因此
  统一跳过 ``release_id IS NOT NULL`` 的记录（release 本身不回放）；
- **全放行/全拒绝**：一个请求涉及的全部桶都满足才放行，任一超限全部
  不扣减，与线上 ``consume`` 语义一致；
- 状态仅保存在本模块的数据结构内，调用方只做只读 SELECT。
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field

# 模拟区间最大允许跨度：7 天
MAX_RANGE_SECONDS = 7 * 24 * 3600

# 判定翻转明细最多返回条数（按回放顺序截取）
FLIP_DETAIL_LIMIT = 100


@dataclass
class _BucketState:
    """单个桶的纯内存滚动窗口状态：生效中的消费事件 (created_at, amount)。"""

    limit: int
    window: float
    events: list[tuple[float, int]] = field(default_factory=list)

    def prune(self, now: float) -> None:
        """丢弃已滑出窗口的事件；事件整体有序，只需弹出过期前缀。

        与线上查询一致，边界为严格不等号：created_at > now - window
        才在窗口内，恰好在边界上的事件视为已过期。
        """
        boundary = now - self.window
        keep_from = 0
        for ts, _ in self.events:
            if ts > boundary:
                break
            keep_from += 1
        if keep_from:
            del self.events[:keep_from]

    def used(self, now: float) -> int:
        self.prune(now)
        return sum(amount for _, amount in self.events)


def warm_start_for(configs: dict[str, tuple[int, float]], start: float) -> float:
    """从 from 向前取各桶窗口最大值作为预热起点。"""
    max_window = max((window for _, window in configs.values()), default=0.0)
    return start - max_window


def simulate(
    rows: list,
    *,
    configs: dict[str, tuple[int, float]],
    start: float,
    end: float,
) -> dict:
    """执行内存回放，返回统计结果。

    参数
    ----
    rows:
        已按 ``(created_at, request_id)`` 升序取出的历史消费记录，每行需
        提供字段：``request_id`` / ``created_at`` / ``payload``（规范化
        载荷 JSON，含 ``buckets`` 列表）/ ``allowed``（原始固化判定）/
        ``release_id``。
    configs:
        模拟使用的桶配置 ``key -> (limit, window_seconds)``（候选已覆盖
        当前配置）。
    start, end:
        统计区间 ``[start, end)``（epoch 秒）；``[warm_start, start)``
        为预热期。
    """
    warm_start = warm_start_for(configs, start)

    sim: dict[str, _BucketState] = {
        key: _BucketState(limit=limit, window=window)
        for key, (limit, window) in configs.items()
    }

    # 区间统计
    transitions = {
        "allowed_to_allowed": 0,
        "allowed_to_denied": 0,
        "denied_to_allowed": 0,
        "denied_to_denied": 0,
    }
    peaks: dict[str, int] = {}
    flips: list[dict] = []

    def evaluate(states: dict[str, _BucketState], items, now: float) -> bool:
        """多桶 all-or-nothing：任一桶超限即全部拒绝。"""
        for item in items:
            st = states.get(item["key"])
            if st is None:
                # 载荷引用了未知桶（当前配置中不存在）：与线上 404 同义，
                # 模拟不单独产出 404，按拒绝处理
                return False
            if st.used(now) + item["amount"] > st.limit:
                return False
        return True

    def grant(states: dict[str, _BucketState], items, now: float) -> None:
        for item in items:
            st = states[item["key"]]
            st.events.append((now, item["amount"]))

    for row in rows:
        # 已成功释放的消费不回放：release 不处理，其用量贡献在真实世界
        # 已删除，模拟世界也不应计入
        if row["release_id"] is not None:
            continue

        now = float(row["created_at"])
        items = _payload_items(row["payload"])
        original_allowed = bool(row["allowed"])
        in_range = start <= now < end
        # rows 已覆盖 [warm_start, end)；早于预热起点的记录直接忽略
        in_warmup = warm_start <= now < start
        if not in_range and not in_warmup:
            continue

        sim_allowed = evaluate(sim, items, now)
        if sim_allowed:
            grant(sim, items, now)

        if in_warmup:
            # 预热记录只建状态、不计统计
            continue

        # ---- 区间内：累计转移矩阵 ----
        if original_allowed and sim_allowed:
            transitions["allowed_to_allowed"] += 1
        elif original_allowed and not sim_allowed:
            transitions["allowed_to_denied"] += 1
        elif not original_allowed and sim_allowed:
            transitions["denied_to_allowed"] += 1
        else:
            transitions["denied_to_denied"] += 1

        # 每桶模拟峰值：在本次判定时刻（已含滚动过期）的窗口用量
        for item in items:
            st = sim.get(item["key"])
            used = st.used(now) if st is not None else 0
            if used > peaks.get(item["key"], 0):
                peaks[item["key"]] = used

        # 判定翻转明细：仅在结果不同时记录，按回放顺序截取前 100 条
        if original_allowed != sim_allowed:
            flips.append(
                {
                    "request_id": row["request_id"],
                    "created_at": now,
                    "original": "allowed" if original_allowed else "denied",
                    "simulated": "allowed" if sim_allowed else "denied",
                    "buckets": items,
                }
            )

    return {
        "from": int(start),
        "to": int(end),
        "warmup_from": warm_start,
        "transitions": transitions,
        "peak_usage": [
            {
                "key": key,
                "limit": sim[key].limit,
                "window_seconds": sim[key].window,
                "peak_used": peaks.get(key, 0),
            }
            for key in sorted(peaks)
        ],
        "flip_count": len(flips),
        "flip_details": flips[:FLIP_DETAIL_LIMIT],
    }


def _payload_items(payload_json: str) -> list[dict]:
    """从固化载荷 JSON 取出规范化桶列表（已按 key 排序、同键已合并）。"""
    payload = json.loads(payload_json)
    return [
        {"key": str(b["key"]), "amount": int(b["amount"])}
        for b in payload.get("buckets", [])
    ]
