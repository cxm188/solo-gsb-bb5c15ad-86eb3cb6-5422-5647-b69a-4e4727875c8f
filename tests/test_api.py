"""端到端测试：原子扣减、幂等、409、并发不超限、滚动窗口恢复、配置回放模拟。"""
import json
import os
import tempfile
import threading
import time
import uuid

# 必须在导入 app 之前设置环境（config 在导入时读取）
_TMP = tempfile.mkdtemp(prefix="quota-test-")
os.environ["QUOTA_DB_PATH"] = os.path.join(_TMP, "test.db")
os.environ["QUOTA_BUCKETS_CONFIG"] = os.path.join(_TMP, "buckets.json")

with open(os.environ["QUOTA_BUCKETS_CONFIG"], "w") as fh:
    json.dump(
        {"buckets": [{"key": "seeded", "limit": 100, "window_seconds": 60}]},
        fh,
    )

from fastapi.testclient import TestClient  # noqa: E402

from app.main import app  # noqa: E402

# 以上下文管理器方式启动，确保 lifespan（建表 + 播种桶配置）执行
_client_ctx = TestClient(app)
client = _client_ctx.__enter__()


def _rid() -> str:
    return uuid.uuid4().hex


def _bucket(limit: int, window: float) -> str:
    """为单个测试创建独立桶，避免测试间窗口互相干扰。"""
    key = f"b-{_rid()[:12]}"
    r = client.put(f"/v1/buckets/{key}", json={"limit": limit, "window_seconds": window})
    assert r.status_code == 200, r.text
    return key


def _consume(rid: str, key: str, amount: int):
    return client.post("/v1/quota/consume", json={
        "request_id": rid, "buckets": [{"key": key, "amount": amount}],
    })


def _release(rel_id: str, rid: str):
    return client.post("/v1/quota/releases", json={
        "release_id": rel_id, "request_id": rid,
    })


def test_seeded_bucket_and_consume():
    # 启动时从 QUOTA_BUCKETS_CONFIG 播种
    keys = [b["key"] for b in client.get("/v1/buckets").json()["buckets"]]
    assert "seeded" in keys

    rid = _rid()
    r = client.post("/v1/quota/consume", json={
        "request_id": rid,
        "buckets": [{"key": "seeded", "amount": 30}],
    })
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["allowed"] is True
    assert body["request_id"] == rid

    usage = client.get("/v1/buckets/seeded/usage").json()
    assert usage["used"] == 30
    assert usage["remaining"] == 70
    assert usage["limit"] == 100
    assert usage["window_seconds"] == 60


def test_atomic_all_or_nothing():
    """多桶请求中任一超限 → 全部不扣减。"""
    k1, k2 = _bucket(10, 60), _bucket(5, 60)
    r = client.post("/v1/quota/consume", json={
        "request_id": _rid(),
        "buckets": [{"key": k1, "amount": 1}, {"key": k2, "amount": 100}],
    })
    assert r.status_code == 429
    body = r.json()
    assert body["allowed"] is False
    assert [b["key"] for b in body["limited_buckets"]] == [k2]
    # 请求量本身超过限额 → 永远不可重试
    assert body["retry_at"] is None
    assert client.get(f"/v1/buckets/{k1}/usage").json()["used"] == 0
    assert client.get(f"/v1/buckets/{k2}/usage").json()["used"] == 0


def test_denied_returns_retry_time():
    key = _bucket(2, 60)
    assert _consume(_rid(), key, 2).status_code == 200

    r = _consume(_rid(), key, 1)
    assert r.status_code == 429
    body = r.json()
    lb = body["limited_buckets"][0]
    assert lb["key"] == key
    assert lb["used"] == 2 and lb["requested"] == 1 and lb["limit"] == 2
    assert 0 < body["retry_after_seconds"] <= 60
    assert body["retry_at"] is not None
    assert "Retry-After" in r.headers


def test_idempotent_replay():
    key = _bucket(10, 60)
    rid = _rid()
    payload = {"request_id": rid, "buckets": [{"key": key, "amount": 3}]}
    r1 = client.post("/v1/quota/consume", json=payload)
    assert r1.status_code == 200

    r2 = client.post("/v1/quota/consume", json=payload)
    assert r2.status_code == 200
    assert r2.json() == r1.json()  # 原样返回固化判定
    assert r2.headers.get("X-Idempotent-Replay") == "true"
    # 不得重复扣减
    assert client.get(f"/v1/buckets/{key}/usage").json()["used"] == 3


def test_idempotent_replay_of_denied_decision():
    key = _bucket(2, 60)
    assert _consume(_rid(), key, 2).status_code == 200

    rid = _rid()
    payload = {"request_id": rid, "buckets": [{"key": key, "amount": 1}]}
    r1 = client.post("/v1/quota/consume", json=payload)
    assert r1.status_code == 429
    r2 = client.post("/v1/quota/consume", json=payload)
    assert r2.status_code == 429
    assert r2.json() == r1.json()
    assert r2.headers.get("X-Idempotent-Replay") == "true"


def test_conflict_on_changed_payload():
    key = _bucket(10, 60)
    rid = _rid()
    assert _consume(rid, key, 1).status_code == 200
    r = _consume(rid, key, 2)
    assert r.status_code == 409


def test_same_amount_different_bucket_key_is_conflict():
    k1, k2 = _bucket(10, 60), _bucket(10, 60)
    rid = _rid()
    assert _consume(rid, k1, 1).status_code == 200
    assert _consume(rid, k2, 1).status_code == 409


def test_unknown_bucket_404():
    r = client.post("/v1/quota/consume", json={
        "request_id": _rid(), "buckets": [{"key": "nope", "amount": 1}],
    })
    assert r.status_code == 404
    assert client.get("/v1/buckets/nope/usage").status_code == 404


def test_rolling_window_recovers():
    key = _bucket(2, 0.4)
    assert _consume(_rid(), key, 2).status_code == 200
    assert _consume(_rid(), key, 1).status_code == 429
    time.sleep(0.5)  # 窗口 0.4s，等待滚动过期
    assert _consume(_rid(), key, 1).status_code == 200


def test_rolling_window_partial_expiry():
    """窗口滚动时按事件过期逐步释放额度。"""
    key = _bucket(3, 0.6)
    assert _consume(_rid(), key, 2).status_code == 200
    time.sleep(0.35)
    assert _consume(_rid(), key, 1).status_code == 200  # 打满 3/3
    assert _consume(_rid(), key, 1).status_code == 429
    time.sleep(0.35)  # 第一笔(2)过期，释放 2 额度
    assert _consume(_rid(), key, 2).status_code == 200


def test_concurrent_requests_never_exceed_limit():
    """50 个并发请求争抢限额 10 的桶：放行数必须恰好为 10。"""
    key = _bucket(10, 60)
    results = []

    def hit():
        results.append(_consume(_rid(), key, 1).status_code)

    threads = [threading.Thread(target=hit) for _ in range(50)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert results.count(200) == 10, f"放行 {results.count(200)} 次，超出限额 10"
    assert results.count(429) == 40
    assert client.get(f"/v1/buckets/{key}/usage").json()["used"] == 10


def test_same_request_id_concurrent_single_deduction():
    """同一 request_id 并发提交 10 次：只应扣减一次。"""
    key = _bucket(100, 60)
    rid = _rid()
    payload = {"request_id": rid, "buckets": [{"key": key, "amount": 1}]}
    codes = []

    def hit():
        codes.append(client.post("/v1/quota/consume", json=payload).status_code)

    threads = [threading.Thread(target=hit) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert all(c == 200 for c in codes)
    assert client.get(f"/v1/buckets/{key}/usage").json()["used"] == 1


def test_bucket_upsert_updates_config():
    key = _bucket(5, 60)
    r = client.put(f"/v1/buckets/{key}", json={"limit": 7, "window_seconds": 30})
    assert r.status_code == 200
    usage = client.get(f"/v1/buckets/{key}/usage").json()
    assert usage["limit"] == 7 and usage["window_seconds"] == 30


def test_validation_errors():
    key = _bucket(5, 60)
    # amount 必须为正
    assert _consume(_rid(), key, 0).status_code == 422
    # buckets 不能为空
    assert client.post("/v1/quota/consume", json={
        "request_id": _rid(), "buckets": [],
    }).status_code == 422


# ---------------------------------------------------------------- 释放接口

def test_release_restores_all_buckets():
    """首次释放撤销对所有桶的用量贡献，额度恢复。"""
    k1, k2 = _bucket(10, 60), _bucket(20, 60)
    rid = _rid()
    r = client.post("/v1/quota/consume", json={
        "request_id": rid,
        "buckets": [{"key": k1, "amount": 4}, {"key": k2, "amount": 7}],
    })
    assert r.status_code == 200
    assert client.get(f"/v1/buckets/{k1}/usage").json()["used"] == 4
    assert client.get(f"/v1/buckets/{k2}/usage").json()["used"] == 7

    rel = _rid()
    rr = _release(rel, rid)
    assert rr.status_code == 200, rr.text
    body = rr.json()
    assert body["released"] is True
    assert body["release_id"] == rel and body["request_id"] == rid
    assert body["events_reverted"] == 2
    # 载荷经 merge_items 规范化后按 key 排序
    assert sorted(body["released_buckets"], key=lambda b: b["key"]) == sorted(
        [{"key": k1, "amount": 4}, {"key": k2, "amount": 7}],
        key=lambda b: b["key"],
    )
    assert "X-Idempotent-Replay" not in rr.headers

    assert client.get(f"/v1/buckets/{k1}/usage").json()["used"] == 0
    assert client.get(f"/v1/buckets/{k2}/usage").json()["used"] == 0


def test_release_frees_capacity_for_subsequent_consume():
    """释放后新的消费可以使用恢复出的额度。"""
    key = _bucket(3, 60)
    rid1 = _rid()
    assert _consume(rid1, key, 3).status_code == 200
    assert _consume(_rid(), key, 1).status_code == 429

    assert _release(_rid(), rid1).status_code == 200
    r = _consume(_rid(), key, 3)
    assert r.status_code == 200
    assert client.get(f"/v1/buckets/{key}/usage").json()["used"] == 3


def test_release_idempotent_replay():
    """相同 release_id + 相同 request_id 重放：原样返回，不再改动用量。"""
    key = _bucket(10, 60)
    rid = _rid()
    assert _consume(rid, key, 5).status_code == 200
    rel = _rid()
    r1 = _release(rel, rid)
    assert r1.status_code == 200

    # 再制造一笔用量，重放不应影响它
    assert _consume(_rid(), key, 3).status_code == 200
    r2 = _release(rel, rid)
    assert r2.status_code == 200
    assert r2.headers.get("X-Idempotent-Replay") == "true"
    assert r2.json() == r1.json()
    assert client.get(f"/v1/buckets/{key}/usage").json()["used"] == 3


def test_release_retarget_conflict():
    """相同 release_id 改指其他 request_id → 409，且不影响任一目标。"""
    key = _bucket(10, 60)
    rid1, rid2 = _rid(), _rid()
    assert _consume(rid1, key, 2).status_code == 200
    assert _consume(rid2, key, 3).status_code == 200
    rel = _rid()
    assert _release(rel, rid1).status_code == 200

    r = _release(rel, rid2)
    assert r.status_code == 409
    # rid1 已释放（-2），rid2 未被释放，用量只剩 3
    assert client.get(f"/v1/buckets/{key}/usage").json()["used"] == 3


def test_double_release_same_consume_conflict():
    """同一消费请求只能由一个 release_id 成功释放，第二个返回 409。"""
    key = _bucket(10, 60)
    rid = _rid()
    assert _consume(rid, key, 4).status_code == 200
    assert _release(_rid(), rid).status_code == 200

    r = _release(_rid(), rid)
    assert r.status_code == 409
    # 用量不会被重复"撤销"
    assert client.get(f"/v1/buckets/{key}/usage").json()["used"] == 0


def test_release_unknown_request_404():
    r = _release(_rid(), f"missing-{_rid()}")
    assert r.status_code == 404


def test_release_denied_request_conflict():
    """原判定为拒绝（429）的消费不可释放 → 409。"""
    key = _bucket(1, 60)
    assert _consume(_rid(), key, 1).status_code == 200
    rid = _rid()
    assert _consume(rid, key, 1).status_code == 429
    assert _release(_rid(), rid).status_code == 409
    assert client.get(f"/v1/buckets/{key}/usage").json()["used"] == 1


def test_consume_replay_after_release_returns_frozen_result():
    """释放后重放原 consume：仍返回已固化的放行结果，且不再次扣减。"""
    key = _bucket(10, 60)
    rid = _rid()
    payload = {"request_id": rid, "buckets": [{"key": key, "amount": 6}]}
    r1 = client.post("/v1/quota/consume", json=payload)
    assert r1.status_code == 200
    assert _release(_rid(), rid).status_code == 200
    assert client.get(f"/v1/buckets/{key}/usage").json()["used"] == 0

    r2 = client.post("/v1/quota/consume", json=payload)
    assert r2.status_code == 200
    assert r2.headers.get("X-Idempotent-Replay") == "true"
    assert r2.json() == r1.json()
    # 重放不得再次扣减
    assert client.get(f"/v1/buckets/{key}/usage").json()["used"] == 0


def test_release_atomic_restores_every_bucket():
    """释放是原子的：多桶用量全部恢复，不存在部分恢复。"""
    keys = [_bucket(5, 60) for _ in range(5)]
    rid = _rid()
    r = client.post("/v1/quota/consume", json={
        "request_id": rid,
        "buckets": [{"key": k, "amount": 3} for k in keys],
    })
    assert r.status_code == 200
    assert _release(_rid(), rid).status_code == 200
    for k in keys:
        assert client.get(f"/v1/buckets/{k}/usage").json()["used"] == 0


def test_concurrent_releases_only_one_wins():
    """不同 release_id 并发释放同一消费：仅一个 200，其余 409。"""
    key = _bucket(10, 60)
    rid = _rid()
    assert _consume(rid, key, 5).status_code == 200

    outcomes: list[tuple[str, int]] = []
    barrier = threading.Barrier(20)

    def hit(rel: str):
        barrier.wait()
        outcomes.append((rel, _release(rel, rid).status_code))

    threads = [threading.Thread(target=hit, args=(_rid(),)) for _ in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert [c for _, c in outcomes].count(200) == 1
    assert [c for _, c in outcomes].count(409) == 19
    # 只删除了一次事件，用量为 0 而非负数
    assert client.get(f"/v1/buckets/{key}/usage").json()["used"] == 0


def test_concurrent_same_release_id_single_result():
    """同一 release_id 并发释放同一消费：全部 200，只执行一次撤销。"""
    key = _bucket(10, 60)
    rid = _rid()
    assert _consume(rid, key, 5).status_code == 200

    rel = _rid()
    codes = []
    barrier = threading.Barrier(15)

    def hit():
        barrier.wait()
        codes.append(_release(rel, rid).status_code)

    threads = [threading.Thread(target=hit) for _ in range(15)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert all(c == 200 for c in codes)
    assert client.get(f"/v1/buckets/{key}/usage").json()["used"] == 0


def test_release_then_new_consume_replay_interaction():
    """释放后：旧 consume 重放不扣减，新 request_id 可正常占用恢复额度。"""
    key = _bucket(5, 60)
    rid_old = _rid()
    assert _consume(rid_old, key, 5).status_code == 200
    assert _release(_rid(), rid_old).status_code == 200

    # 旧单重放不占额度
    assert _consume(rid_old, key, 5).status_code == 200
    assert client.get(f"/v1/buckets/{key}/usage").json()["used"] == 0
    # 新单可以打满恢复出的额度
    rid_new = _rid()
    assert _consume(rid_new, key, 5).status_code == 200
    assert client.get(f"/v1/buckets/{key}/usage").json()["used"] == 5
    assert _consume(_rid(), key, 1).status_code == 429


def test_release_validation_errors():
    r = client.post("/v1/quota/releases", json={
        "release_id": "", "request_id": "x",
    })
    assert r.status_code == 422
    r = client.post("/v1/quota/releases", json={"release_id": "r1"})
    assert r.status_code == 422


# ---------------------------------------------------------------- 哈希审计链

def _audit(after_seq: int = 0, limit: int = 1000):
    return client.get(
        "/v1/audit", params={"after_seq": after_seq, "limit": limit}
    )


def test_audit_hash_rule_pure():
    """链哈希规则：64 个 0 起链；SHA-256(prev 原始32字节 + canonical JSON)。"""
    import hashlib

    from app import audit

    assert audit.GENESIS_HASH == "0" * 64

    event = {
        "op": "consume",
        "business_id": "rid-1",
        "result": "allowed",
        "occurred_at": 1789657657.47,
    }
    event_json = audit.canonical_event_json(event)
    # 键排序且无多余空白
    assert event_json == (
        '{"business_id":"rid-1","occurred_at":1789657657.47,'
        '"op":"consume","result":"allowed"}'
    )
    digest = audit.compute_hash(audit.GENESIS_HASH, event_json)
    assert digest == hashlib.sha256(
        bytes(32) + event_json.encode("utf-8")
    ).hexdigest()

    # 第二条引用第一条 hash
    event2 = dict(event, business_id="rid-2", result="denied")
    digest2 = audit.compute_hash(digest, audit.canonical_event_json(event2))
    assert digest2 == hashlib.sha256(
        bytes.fromhex(digest)
        + audit.canonical_event_json(event2).encode("utf-8")
    ).hexdigest()
    assert digest2 != digest


def test_audit_first_record_anchors_at_genesis():
    """链首记录 seq=1、prev_hash 为 64 个 0。"""
    r = _audit(0, 1)
    assert r.status_code == 200
    first = r.json()["records"][0]
    assert first["seq"] == 1
    assert first["prev_hash"] == "0" * 64
    assert len(first["hash"]) == 64
    assert set(first["hash"]) <= set("0123456789abcdef")


def test_audit_consume_appended_once_allow_and_deny():
    """首次放行与首次拒绝各记一条；幂等重放不重复记录。"""
    before = client.get("/v1/audit/verify").json()["checked_count"]

    # 放行 + 重放
    k_allow = _bucket(10, 60)
    rid_allow = _rid()
    assert _consume(rid_allow, k_allow, 3).status_code == 200
    assert _consume(rid_allow, k_allow, 3).status_code == 200

    # 先用一笔放行打满桶，再触发拒绝 + 拒绝重放
    k_deny = _bucket(1, 60)
    assert _consume(_rid(), k_deny, 1).status_code == 200
    rid_denied = _rid()
    assert _consume(rid_denied, k_deny, 1).status_code == 429
    assert _consume(rid_denied, k_deny, 1).status_code == 429

    after = client.get("/v1/audit/verify").json()["checked_count"]
    # 放行 2 笔 + 拒绝 1 笔 = 3 条；两次重放不记录
    assert after - before == 3

    records = _audit(before, 10).json()["records"]
    by_biz = {rec["event"]["business_id"]: rec for rec in records}
    allow_rec = by_biz[rid_allow]
    assert allow_rec["event"]["op"] == "consume"
    assert allow_rec["event"]["result"] == "allowed"
    assert isinstance(allow_rec["event"]["occurred_at"], (int, float))
    deny_rec = by_biz[rid_denied]
    assert deny_rec["event"]["op"] == "consume"
    assert deny_rec["event"]["result"] == "denied"

    # 链关系：逐条引用上一条 hash
    prev = records[0]["prev_hash"]
    for rec in records:
        assert rec["prev_hash"] == prev
        prev = rec["hash"]


def test_audit_release_appended_only_on_first_success():
    """仅首次成功释放记一条；重放与失败释放（404/409）均不记录。"""
    before = client.get("/v1/audit/verify").json()["checked_count"]

    key = _bucket(10, 60)
    rid = _rid()
    assert _consume(rid, key, 4).status_code == 200
    rel = _rid()
    assert _release(rel, rid).status_code == 200
    assert _release(rel, rid).status_code == 200  # 成功重放，不重复记录

    # 失败释放：原单不存在(404)、重复释放(409)、释放被拒消费(409)
    assert _release(_rid(), f"missing-{_rid()}").status_code == 404
    assert _release(_rid(), rid).status_code == 409
    rid_denied = _rid()
    k2 = _bucket(1, 60)
    assert _consume(rid_denied, k2, 1).status_code == 200
    rid_blocked = _rid()
    assert _consume(rid_blocked, k2, 1).status_code == 429
    assert _release(_rid(), rid_blocked).status_code == 409

    after = client.get("/v1/audit/verify").json()["checked_count"]
    # consume 放行 rid、consume 放行 rid_denied、consume 拒绝 rid_blocked、
    # release rel —— 共 4 条
    assert after - before == 4

    rec = [
        r for r in _audit(before, 10).json()["records"]
        if r["event"]["business_id"] == rel
    ]
    assert len(rec) == 1
    event = rec[0]["event"]
    assert event["op"] == "release"
    assert event["result"] == "released"
    assert event["business_id"] == rel
    assert isinstance(event["occurred_at"], (int, float))
    assert set(event) == {"op", "business_id", "result", "occurred_at"}


def test_audit_conflict_and_unknown_bucket_not_recorded():
    """409 幂等冲突与 404 桶未配置不产生审计记录。"""
    before = client.get("/v1/audit/verify").json()["checked_count"]

    key = _bucket(10, 60)
    rid = _rid()
    assert _consume(rid, key, 1).status_code == 200
    assert _consume(rid, key, 2).status_code == 409       # 载荷冲突
    assert client.post("/v1/quota/consume", json={
        "request_id": _rid(), "buckets": [{"key": "nope-bucket", "amount": 1}],
    }).status_code == 404

    assert client.get("/v1/audit/verify").json()["checked_count"] - before == 1


def test_audit_pagination_and_head():
    """GET /v1/audit 按 after_seq/limit 升序分页，并返回链头。"""
    head_before = _audit(0, 1).json()["head"]["seq"]
    key = _bucket(100, 60)
    rids = []
    for _ in range(3):
        rid = _rid()
        rids.append(rid)
        assert _consume(rid, key, 1).status_code == 200

    page1 = _audit(head_before, 2).json()
    assert [r["seq"] for r in page1["records"]] == [
        head_before + 1, head_before + 2,
    ]
    assert page1["next_after_seq"] == head_before + 2
    assert page1["head"]["seq"] == head_before + 3

    page2 = _audit(head_before + 2, 2).json()
    assert [r["event"]["business_id"] for r in page2["records"]] == [rids[2]]
    assert page2["records"][0]["prev_hash"] == page1["records"][1]["hash"]

    # 追上链头后再翻页为空，链头保持不变
    empty = _audit(head_before + 3, 2).json()
    assert empty["records"] == []
    assert empty["next_after_seq"] == head_before + 3
    assert empty["head"] == page1["head"]

    verify = client.get("/v1/audit/verify").json()
    assert verify["head_hash"] == page1["head"]["hash"]


def test_audit_query_validation():
    r = client.get("/v1/audit", params={"after_seq": -1})
    assert r.status_code == 422
    r = client.get("/v1/audit", params={"limit": 0})
    assert r.status_code == 422
    r = client.get("/v1/audit", params={"limit": 1001})
    assert r.status_code == 422


def test_audit_verify_valid_full_chain():
    """所有业务测试产生的记录构成一条完整有效链。"""
    body = client.get("/v1/audit/verify").json()
    assert set(body) == {
        "valid", "checked_count", "head_hash", "first_invalid_seq",
    }
    assert body["valid"] is True
    assert body["first_invalid_seq"] is None
    assert len(body["head_hash"]) == 64
    assert body["checked_count"] >= 1

    listing = _audit(0, 1000).json()
    assert len(listing["records"]) == body["checked_count"]
    assert listing["head"]["hash"] == body["head_hash"]


def test_audit_verify_detects_tampering():
    """篡改事件内容或前序指针：verify 报告首个失配 seq；恢复后重新有效。"""
    from app.db import connect

    listing = _audit(0, 1000).json()
    records = listing["records"]
    target = records[-1]
    original = None

    conn = connect()
    try:
        original = conn.execute(
            "SELECT event_json, prev_hash FROM audit_log WHERE seq = ?",
            (target["seq"],),
        ).fetchone()

        # 1) 篡改事件内容：存储 hash 与重算值不符
        conn.execute(
            "UPDATE audit_log SET event_json = ? WHERE seq = ?",
            (
                '{"business_id":"forged","occurred_at":0,'
                '"op":"consume","result":"allowed"}',
                target["seq"],
            ),
        )
        body = client.get("/v1/audit/verify").json()
        assert body["valid"] is False
        assert body["first_invalid_seq"] == target["seq"]
        assert body["checked_count"] == len(records)

        # 恢复后再次有效
        conn.execute(
            "UPDATE audit_log SET event_json = ? WHERE seq = ?",
            (original["event_json"], target["seq"]),
        )
        assert client.get("/v1/audit/verify").json()["valid"] is True

        # 2) 篡改前序指针：prev_hash 与上一条不一致
        conn.execute(
            "UPDATE audit_log SET prev_hash = ? WHERE seq = ?",
            ("f" * 64, target["seq"]),
        )
        body = client.get("/v1/audit/verify").json()
        assert body["valid"] is False
        assert body["first_invalid_seq"] == target["seq"]
    finally:
        # 务必恢复，保证全链对后续检查保持有效
        if original is not None:
            conn.execute(
                "UPDATE audit_log SET event_json = ?, prev_hash = ? WHERE seq = ?",
                (original["event_json"], original["prev_hash"], target["seq"]),
            )
        conn.close()

    assert client.get("/v1/audit/verify").json()["valid"] is True


# ---------------------------------------------------------------- 回放模拟

def _insert_request(rid: str, key: str, amount: int, created_at: float,
                    allowed: bool):
    """直接写入一条历史消费固化记录（绕过 created_at=now）。

    模拟只读取 requests 表中首次消费固化的载荷与判定，不依赖
    usage_events，因此这里只构造 requests 记录。
    """
    from app.db import connect

    payload = {"request_id": rid,
               "buckets": [{"key": key, "amount": amount}]}
    decision = {"request_id": rid, "allowed": allowed,
                "decided_at": created_at}
    conn = connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "INSERT INTO requests (request_id, payload_hash, payload, "
            "decision, allowed, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            (rid, "0" * 64, json.dumps(payload), json.dumps(decision),
             1 if allowed else 0, created_at),
        )
        conn.execute("COMMIT")
    finally:
        conn.close()


def _insert_request_multi(rid: str, items: list[dict], created_at: float,
                          allowed: bool):
    from app.db import connect

    payload = {"request_id": rid, "buckets": items}
    decision = {"request_id": rid, "allowed": allowed,
                "decided_at": created_at}
    conn = connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "INSERT INTO requests (request_id, payload_hash, payload, "
            "decision, allowed, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            (rid, "0" * 64, json.dumps(payload), json.dumps(decision),
             1 if allowed else 0, created_at),
        )
        conn.execute("COMMIT")
    finally:
        conn.close()


def _simulate(payload: dict):
    return client.post("/v1/quota/simulations", json=payload)


def test_simulation_transitions_and_effective_config():
    """allow→allow / allow→deny / deny→deny 转移计数、生效配置与峰值。"""
    key = _bucket(10, 100.0)
    base = 1_000_000
    # 预热：t=950 放行 8；窗口 100，在 t=1000 仍占用
    _insert_request("sim-w", key, 8, base - 50, True)
    # 区间内：放行 1（9/10）、再放行 1（10/10）、
    # 实际拒绝且模拟仍拒绝 1 笔、实际放行但模拟拒绝 1 笔
    _insert_request("sim-a", key, 1, base + 10, True)
    _insert_request("sim-b", key, 1, base + 20, True)
    _insert_request("sim-c", key, 1, base + 30, False)
    _insert_request("sim-d", key, 1, base + 40, True)

    r = _simulate({"from": base, "to": base + 100,
                   "buckets": [{"key": key, "limit": 10,
                                "window_seconds": 100}]})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["from"] == base and body["to"] == base + 100
    assert body["warmup_seconds"] == max(
        b["window_seconds"] for b in body["effective_buckets"]
    )
    eff = {b["key"]: b for b in body["effective_buckets"]}
    assert eff[key] == {"key": key, "limit": 10, "window_seconds": 100.0}
    assert body["transitions"] == {
        "allowed_to_allowed": 2,
        "allowed_to_denied": 1,
        "denied_to_allowed": 0,
        "denied_to_denied": 1,
    }
    # 模拟峰值：8（预热残留）+1+1 = 10；被拒请求不占用
    peaks = {p["key"]: p["simulated_peak"] for p in body["bucket_peaks"]}
    assert peaks == {key: 10}
    assert body["records_replayed"] == 4
    assert body["warmup_records"] == 1
    assert body["flip_details_truncated"] is False

    flip = body["flip_details"][0]
    assert flip["request_id"] == "sim-d"
    assert flip["created_at"] == base + 40
    assert flip["actual_decision"] == "allowed"
    assert flip["simulated_decision"] == "denied"
    assert flip["buckets"] == [{"key": key, "amount": 1}]


def test_simulation_denied_to_allowed_on_window_shrink():
    """候选窗口变小：实际满窗拒绝的请求在模拟中窗口已滚动放行。"""
    key = _bucket(1, 100.0)
    base = 2_000_000
    # 预热事件早于候选窗口(50s)、但在实际窗口(100s)内：
    # 实际 t=base 时它仍占用 → simx-deny 被拒；模拟窗口 50s 已过期 → 放行
    _insert_request("simx-old", key, 1, base - 70, True)
    _insert_request("simx-deny", key, 1, base, False)
    _insert_request("simx-ok", key, 1, base + 60, True)

    r = _simulate({"from": base, "to": base + 100,
                   "buckets": [{"key": key, "limit": 1,
                                "window_seconds": 50}]})
    assert r.status_code == 200, r.text
    body = r.json()
    # 预热期取全部生效桶窗口的最大值（保守覆盖），不短于候选窗口
    assert body["warmup_seconds"] >= 50
    assert body["warmup_seconds"] == max(
        b["window_seconds"] for b in body["effective_buckets"]
    )
    assert body["warmup_records"] == 1
    assert body["transitions"] == {
        "allowed_to_allowed": 1,
        "allowed_to_denied": 0,
        "denied_to_allowed": 1,
        "denied_to_denied": 0,
    }
    flips = body["flip_details"]
    assert len(flips) == 1
    assert flips[0]["request_id"] == "simx-deny"
    assert flips[0]["actual_decision"] == "denied"
    assert flips[0]["simulated_decision"] == "allowed"


def test_simulation_uncovered_bucket_keeps_current_config():
    """未给候选的桶沿用当前配置。"""
    key = _bucket(3, 100.0)
    base = 3_000_000
    _insert_request("keep-a", key, 3, base + 1, True)
    _insert_request("keep-b", key, 1, base + 2, False)

    r = _simulate({"from": base, "to": base + 100, "buckets": []})
    assert r.status_code == 200, r.text
    body = r.json()
    eff = {b["key"]: b for b in body["effective_buckets"]}
    assert eff[key] == {"key": key, "limit": 3, "window_seconds": 100.0}
    assert body["transitions"] == {
        "allowed_to_allowed": 1,
        "allowed_to_denied": 0,
        "denied_to_allowed": 0,
        "denied_to_denied": 1,
    }
    assert body["flip_details"] == []
    assert {p["key"]: p["simulated_peak"]
            for p in body["bucket_peaks"]} == {key: 3}


def test_simulation_releases_are_not_replayed():
    """模拟只回放消费：已释放的预热消费仍占用模拟额度（release 被忽略）。"""
    key = _bucket(1, 100.0)
    base = 4_000_000
    _insert_request("rel-old", key, 1, base - 10, True)
    # 线上随后释放 rel-old，使 rel-new 实际得以放行
    assert _release("rel-1", "rel-old").status_code == 200
    _insert_request("rel-new", key, 1, base + 5, True)

    r = _simulate({"from": base, "to": base + 100, "buckets": []})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["warmup_records"] == 1
    assert body["transitions"]["allowed_to_denied"] == 1
    assert body["transitions"]["allowed_to_allowed"] == 0
    flip = body["flip_details"][0]
    assert flip["request_id"] == "rel-new"
    assert flip["actual_decision"] == "allowed"
    assert flip["simulated_decision"] == "denied"


def test_simulation_multi_bucket_all_or_nothing():
    """多桶请求：任一桶超限则模拟整体拒绝、所有桶均不占用。"""
    k1, k2 = _bucket(5, 100.0), _bucket(5, 100.0)
    base = 5_000_000
    _insert_request_multi("mb-1", [{"key": k1, "amount": 5},
                                   {"key": k2, "amount": 5}], base + 1, True)
    _insert_request_multi("mb-2", [{"key": k1, "amount": 1},
                                   {"key": k2, "amount": 1}], base + 2, True)

    r = _simulate({"from": base, "to": base + 100, "buckets": []})
    assert r.status_code == 200, r.text
    t = r.json()["transitions"]
    assert t["allowed_to_allowed"] == 1
    assert t["allowed_to_denied"] == 1
    peaks = {p["key"]: p["simulated_peak"]
             for p in r.json()["bucket_peaks"]}
    # mb-2 被整体拒绝，两桶峰值都停留在 5
    assert peaks == {k1: 5, k2: 5}
    flip = r.json()["flip_details"][0]
    assert flip["request_id"] == "mb-2"
    assert sorted(b["key"] for b in flip["buckets"]) == sorted([k1, k2])


def test_simulation_warmup_boundary():
    """恰好落在预热起点的事件在区间开始时已过期。"""
    key = _bucket(100, 100.0)
    base = 6_000_000
    _insert_request("wb-edge", key, 5, base - 100, True)   # t=from-window
    _insert_request("wb-in", key, 3, base + 1, True)

    r = _simulate({"from": base, "to": base + 100, "buckets": []})
    body = r.json()
    assert body["warmup_records"] == 1
    peaks = {p["key"]: p["simulated_peak"] for p in body["bucket_peaks"]}
    assert peaks[key] == 3


def test_simulation_flip_details_stable_order_and_limit():
    """翻转明细按 (created_at, request_id) 稳定顺序，最多 100 条。"""
    key = _bucket(1, 1000.0)
    base = 7_000_000
    # 同秒内 request_id 决定顺序：a 放行，b、c 及之后全部在模拟中翻转
    for i, rid in enumerate(["sa", "sb", "sc"] + [f"s{i:03d}" for i in range(120)]):
        _insert_request(rid, key, 1, float(base + i), True)

    r = _simulate({"from": base, "to": base + 1000, "buckets": []})
    body = r.json()
    # 首条 allowed→allowed，其余 122 条全部 allowed→denied
    assert body["transitions"]["allowed_to_allowed"] == 1
    assert body["transitions"]["allowed_to_denied"] == 122
    assert len(body["flip_details"]) == 100
    assert body["flip_details_truncated"] is True
    # 稳定顺序：先按时间（首条翻转是同秒的 sb，而非更晚的记录）
    assert body["flip_details"][0]["request_id"] == "sb"
    assert body["flip_details"][1]["request_id"] == "sc"
    ordered = [(f["created_at"], f["request_id"]) for f in body["flip_details"]]
    assert ordered == sorted(ordered)


def test_simulation_is_read_only():
    """模拟不写业务数据也不追加审计链。"""
    before_req = client.get("/v1/audit/verify").json()["checked_count"]
    key = _bucket(2, 100.0)
    base = 8_000_000
    _insert_request("ro-1", key, 1, base + 1, True)

    usage_before = client.get(f"/v1/buckets/{key}/usage").json()["used"]
    r = _simulate({"from": base, "to": base + 100,
                   "buckets": [{"key": key, "limit": 1,
                                "window_seconds": 10}]})
    assert r.status_code == 200
    # 桶配置未被候选覆盖
    assert client.get(f"/v1/buckets/{key}/usage").json()["used"] == usage_before
    cfg = client.get("/v1/buckets").json()["buckets"]
    assert {b["key"]: (b["limit"], b["window_seconds"]) for b in cfg
            if b["key"] == key} == {key: (2, 100.0)}
    assert client.get("/v1/audit/verify").json()["checked_count"] == before_req


def test_simulation_validation_errors():
    """未知桶/非法范围/跨度超 7 天/候选重复 → 422。"""
    key = _bucket(1, 60.0)

    assert _simulate({"from": 100, "to": 90, "buckets": []}).status_code == 422
    assert _simulate({"from": 100, "to": 100, "buckets": []}).status_code == 422
    assert _simulate({
        "from": 0, "to": 7 * 24 * 3600 + 1, "buckets": [],
    }).status_code == 422
    # 恰好 7 天合法
    assert _simulate({
        "from": 0, "to": 7 * 24 * 3600, "buckets": [],
    }).status_code == 200
    # 未知桶
    assert _simulate({"from": 0, "to": 100, "buckets": [
        {"key": "not-a-bucket", "limit": 1, "window_seconds": 60},
    ]}).status_code == 422
    # 候选桶重复
    assert _simulate({"from": 0, "to": 100, "buckets": [
        {"key": key, "limit": 1, "window_seconds": 60},
        {"key": key, "limit": 2, "window_seconds": 60},
    ]}).status_code == 422
    # 候选值非法（pydantic）
    assert _simulate({"from": 0, "to": 100, "buckets": [
        {"key": key, "limit": 0, "window_seconds": 60},
    ]}).status_code == 422
    # 缺少必填字段
    assert _simulate({"to": 100, "buckets": []}).status_code == 422


def test_simulation_empty_range():
    """区间内无任何记录：转移全 0、峰值为空，预热仍正常执行。"""
    key = _bucket(1, 60.0)
    _insert_request("far-away", key, 1, 50.0, True)
    r = _simulate({"from": 9_000_000, "to": 9_000_100, "buckets": []})
    assert r.status_code == 200
    body = r.json()
    assert set(body["transitions"].values()) == {0}
    assert body["bucket_peaks"] == []
    assert body["flip_details"] == []
    assert body["warmup_records"] == 0
    assert body["records_replayed"] == 0


def test_simulation_peak_includes_warmup_residual_on_denials():
    """区间内请求全部被模拟拒绝时，峰值仍反映预热残留占用。"""
    key = _bucket(10, 100.0)
    base = 10_000_000
    _insert_request("res-w", key, 7, base - 10, True)   # 区间起点仍残留 7
    _insert_request("res-1", key, 4, base + 1, True)    # 模拟拒绝
    _insert_request("res-2", key, 4, base + 2, False)   # 模拟仍拒绝

    r = _simulate({"from": base, "to": base + 100,
                   "buckets": [{"key": key, "limit": 10,
                                "window_seconds": 100}]})
    assert r.status_code == 200, r.text
    peaks = {p["key"]: p["simulated_peak"]
             for p in r.json()["bucket_peaks"]}
    # 两次判定时刻占用均为 7（预热残留），峰值 7
    assert peaks == {key: 7}


def test_simulation_stale_payload_unknown_bucket_422():
    """历史固化载荷引用已不存在的桶：无法回放，422 且不留痕。"""
    from app.db import connect

    ghost = f"ghost-{_rid()[:10]}"
    assert client.put(f"/v1/buckets/{ghost}", json={
        "limit": 1, "window_seconds": 60,
    }).status_code == 200
    base = 11_000_000
    _insert_request("ghost-1", ghost, 1, base + 1, True)
    # 直接删除桶（无删除接口），模拟时历史载荷引用即失效
    conn = connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute("DELETE FROM buckets WHERE key = ?", (ghost,))
        conn.execute("COMMIT")
    finally:
        conn.close()

    before = client.get("/v1/audit/verify").json()["checked_count"]
    r = _simulate({"from": base, "to": base + 100, "buckets": []})
    assert r.status_code == 422
    assert ghost in r.json()["detail"]
    assert client.get("/v1/audit/verify").json()["checked_count"] == before
