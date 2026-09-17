"""端到端测试：原子扣减、幂等、409、并发不超限、滚动窗口恢复。"""
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

