"""POST /v1/quota/simulations 端到端测试。

使用独立临时库，直接向 requests/usage_events 播种受控历史（模拟历史上
已经固化的首次消费），随后调用模拟接口验证：参数校验、预热期、滚动过期、
多桶 all-or-nothing、翻转明细顺序/截断、已释放消费不回放、纯只读无副作用。
"""
import json
import os
import tempfile
import uuid
import itertools

import pytest

# 必须在导入 app 之前设置环境（config 在导入时读取）
_TMP = tempfile.mkdtemp(prefix="quota-sim-test-")
os.environ["QUOTA_DB_PATH"] = os.path.join(_TMP, "test.db")
os.environ["QUOTA_BUCKETS_CONFIG"] = os.path.join(_TMP, "buckets.json")

with open(os.environ["QUOTA_BUCKETS_CONFIG"], "w") as fh:
    json.dump({"buckets": []}, fh)

from fastapi.testclient import TestClient  # noqa: E402

from app.db import connect  # noqa: E402
from app.main import app  # noqa: E402
from app.quota import merge_items, payload_hash  # noqa: E402

_client_ctx = TestClient(app)
client = _client_ctx.__enter__()

# 固定的 epoch 基准，避免依赖 time.time()
T0 = 1_700_000_000


@pytest.fixture(autouse=True)
def _isolated_time_base(monkeypatch):
    """每个用例使用互不相交的时间段，避免共享库中的历史记录互相串扰。"""
    base = globals()["T0"] + next(_test_counter) * 1_000_000
    monkeypatch.setitem(globals(), "T0", base)
    yield


_test_counter = itertools.count()


def _rid() -> str:
    return uuid.uuid4().hex


def _make_bucket(limit: int, window: float) -> str:
    key = f"b-{_rid()[:12]}"
    r = client.put(f"/v1/buckets/{key}", json={"limit": limit, "window_seconds": window})
    assert r.status_code == 200, r.text
    return key


def _seed_consume(
    rid: str,
    created_at: float,
    buckets: list[tuple[str, int]],
    allowed: bool,
    *,
    released: bool = False,
):
    """直接播种一条已固化的首次消费记录（与线上 consume 同构）。

    放行的记录同时写 usage_events；released=True 标记已被释放（事件已删除）。
    """
    items = merge_items([{"key": k, "amount": a} for k, a in buckets])
    payload = {"request_id": rid, "buckets": items}
    payload_json = json.dumps(payload, sort_keys=True)
    conn = connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "INSERT INTO requests "
            "(request_id, payload_hash, payload, decision, allowed, release_id, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                rid,
                payload_hash(items),
                payload_json,
                json.dumps({"request_id": rid, "allowed": allowed}),
                1 if allowed else 0,
                f"rel-{rid}" if released else None,
                created_at,
            ),
        )
        if allowed and not released:
            for it in items:
                conn.execute(
                    "INSERT INTO usage_events (bucket_key, request_id, amount, created_at) "
                    "VALUES (?, ?, ?, ?)",
                    (it["key"], rid, it["amount"], created_at),
                )
        conn.execute("COMMIT")
    finally:
        conn.close()


def _sim(payload: dict, expected: int = 200) -> dict:
    r = client.post("/v1/quota/simulations", json=payload)
    assert r.status_code == expected, r.text
    return r.json()


# ---------------------------------------------------------------- 参数校验

def test_validation_unknown_bucket():
    r = client.post("/v1/quota/simulations", json={
        "from": T0, "to": T0 + 60,
        "buckets": [{"key": "no-such-bucket", "limit": 1, "window_seconds": 60}],
    })
    assert r.status_code == 422
    assert "未知桶" in r.text


def test_validation_bad_range_and_span():
    key = _make_bucket(10, 60)
    cand = [{"key": key, "limit": 10, "window_seconds": 60}]
    # from >= to
    assert client.post("/v1/quota/simulations", json={
        "from": 100, "to": 100, "buckets": cand,
    }).status_code == 422
    assert client.post("/v1/quota/simulations", json={
        "from": 100, "to": 99, "buckets": cand,
    }).status_code == 422
    # 跨度恰好 7 天 + 1 秒
    assert client.post("/v1/quota/simulations", json={
        "from": 0, "to": 7 * 24 * 3600 + 1, "buckets": cand,
    }).status_code == 422
    # 负 from（schema 层）
    assert client.post("/v1/quota/simulations", json={
        "from": -1, "to": 10, "buckets": cand,
    }).status_code == 422


def test_validation_duplicate_candidate_and_bad_config():
    key = _make_bucket(10, 60)
    assert client.post("/v1/quota/simulations", json={
        "from": 0, "to": 10,
        "buckets": [
            {"key": key, "limit": 5, "window_seconds": 60},
            {"key": key, "limit": 8, "window_seconds": 30},
        ],
    }).status_code == 422
    assert client.post("/v1/quota/simulations", json={
        "from": 0, "to": 10,
        "buckets": [{"key": key, "limit": 0, "window_seconds": 60}],
    }).status_code == 422


def test_span_exactly_seven_days_ok():
    key = _make_bucket(10, 60)
    body = _sim({
        "from": T0, "to": T0 + 7 * 24 * 3600,
        "buckets": [{"key": key, "limit": 10, "window_seconds": 60}],
    })
    assert body["from"] == T0 and body["to"] == T0 + 7 * 24 * 3600


# ---------------------------------------------------------------- 转移统计

def test_empty_range():
    key = _make_bucket(10, 60)
    body = _sim({"from": T0 + 10_000_000, "to": T0 + 10_000_060,
                 "buckets": [{"key": key, "limit": 9, "window_seconds": 60}]})
    t = body["transitions"]
    assert t == {
        "allowed_to_allowed": 0, "allowed_to_denied": 0,
        "denied_to_allowed": 0, "denied_to_denied": 0,
    }
    assert body["flip_count"] == 0 and body["flip_details"] == []
    assert body["peak_usage"] == []


def test_no_flip_when_config_unchanged():
    """区间内全放行，候选与当前配置一致：全部 allowed→allowed。"""
    key = _make_bucket(10, 60)
    for i in range(3):
        _seed_consume(f"rid-{_rid()}", T0 + i, [(key, 1)], True)
    body = _sim({"from": T0, "to": T0 + 60,
                 "buckets": [{"key": key, "limit": 10, "window_seconds": 60}]})
    t = body["transitions"]
    assert t["allowed_to_allowed"] == 3
    assert t["allowed_to_denied"] == 0
    assert t["denied_to_allowed"] == 0
    assert t["denied_to_denied"] == 0
    assert body["flip_count"] == 0
    peak = [p for p in body["peak_usage"] if p["key"] == key][0]
    assert peak["peak_used"] == 3
    assert peak["limit"] == 10 and peak["window_seconds"] == 60


def test_lower_limit_causes_allowed_to_denied():
    """收紧限额：原始放行的第 3 笔在模拟中被拒绝。"""
    key = _make_bucket(3, 60)
    rids = []
    for i in range(3):
        rid = f"rid-{_rid()}"
        rids.append(rid)
        _seed_consume(rid, T0 + i, [(key, 1)], True)
    body = _sim({"from": T0, "to": T0 + 60,
                 "buckets": [{"key": key, "limit": 2, "window_seconds": 60}]})
    t = body["transitions"]
    assert t["allowed_to_allowed"] == 2
    assert t["allowed_to_denied"] == 1
    assert t["denied_to_allowed"] == 0
    assert t["denied_to_denied"] == 0
    # 模拟只放行了 2 笔，峰值为 2
    peak = [p for p in body["peak_usage"] if p["key"] == key][0]
    assert peak["peak_used"] == 2
    # 翻转明细按回放顺序，翻转的是最后一笔
    assert body["flip_count"] == 1
    flip = body["flip_details"][0]
    assert flip["request_id"] == rids[2]
    assert flip["created_at"] == T0 + 2
    assert flip["original"] == "allowed" and flip["simulated"] == "denied"
    assert flip["buckets"] == [{"key": key, "amount": 1}]


def test_higher_limit_causes_denied_to_allowed():
    """放宽限额：原始被拒的消费在模拟中放行。"""
    key = _make_bucket(2, 60)
    _seed_consume(f"rid-{_rid()}", T0, [(key, 2)], True)
    denied_rid = f"rid-{_rid()}"
    _seed_consume(denied_rid, T0 + 1, [(key, 1)], False)

    body = _sim({"from": T0, "to": T0 + 60,
                 "buckets": [{"key": key, "limit": 3, "window_seconds": 60}]})
    t = body["transitions"]
    assert t["allowed_to_allowed"] == 1
    assert t["denied_to_allowed"] == 1
    assert t["allowed_to_denied"] == 0
    assert t["denied_to_denied"] == 0
    flip = body["flip_details"][0]
    assert flip["request_id"] == denied_rid
    assert flip["original"] == "denied" and flip["simulated"] == "allowed"
    peak = [p for p in body["peak_usage"] if p["key"] == key][0]
    assert peak["peak_used"] == 3


def test_denied_to_denied_when_still_over():
    """限额依旧不足：原始拒绝、模拟仍拒绝（首笔占满容量）。"""
    key = _make_bucket(2, 60)
    _seed_consume(f"rid-{_rid()}", T0, [(key, 2)], True)
    _seed_consume(f"rid-{_rid()}", T0 + 1, [(key, 1)], False)
    # 候选配置与当前一致：首笔放行占满 2/2，第二笔两边都拒绝
    body = _sim({"from": T0, "to": T0 + 60,
                 "buckets": [{"key": key, "limit": 2, "window_seconds": 60}]})
    t = body["transitions"]
    assert t["allowed_to_allowed"] == 1
    assert t["denied_to_denied"] == 1


# ---------------------------------------------------------------- 预热期

def test_warmup_records_build_state_but_not_counted():
    """预热期的消费建立窗口状态，但不出现在任何转移计数中。"""
    key = _make_bucket(10, 100.0)
    # 区间 [T0, T0+50)，最大窗口 100 → 预热从 T0-100 起
    _seed_consume(f"rid-warm-{_rid()}", T0 - 100, [(key, 9)], True)
    _seed_consume(f"rid-warm2-{_rid()}", T0 - 50, [(key, 1)], True)
    # 区间内再申请 1：用量 10 已满（T0-100 的事件恰在边界上视为过期，
    # T0-50 的事件仍在窗口内 → used=1），限额收紧为 1 时应拒绝
    in_rid = f"rid-in-{_rid()}"
    _seed_consume(in_rid, T0 + 10, [(key, 1)], True)  # 原始（限额10）放行

    body = _sim({"from": T0, "to": T0 + 50,
                 "buckets": [{"key": key, "limit": 1, "window_seconds": 100.0}]})
    assert body["warmup_from"] == T0 - 100
    t = body["transitions"]
    # 仅区间内 1 条计入；它在模拟中因预热状态（used=1）而被拒绝
    total = sum(t.values())
    assert total == 1
    assert t["allowed_to_denied"] == 1
    assert body["flip_details"][0]["request_id"] == in_rid


def test_warmup_boundary_event_expires():
    """created_at 恰好在预热起点上：区间开始时刻已滑出窗口（严格 > 边界）。"""
    key = _make_bucket(10, 100.0)
    _seed_consume(f"rid-edge-{_rid()}", T0 - 100, [(key, 5)], True)
    _seed_consume(f"rid-in-{_rid()}", T0 + 1, [(key, 1)], True)
    body = _sim({"from": T0, "to": T0 + 50,
                 "buckets": [{"key": key, "limit": 1, "window_seconds": 100.0}]})
    # 边界事件已过期，区间内申请 1 恰好放行
    assert body["transitions"]["allowed_to_allowed"] == 1
    peak = [p for p in body["peak_usage"] if p["key"] == key][0]
    assert peak["peak_used"] == 1


def test_pre_warmup_records_ignored():
    """早于预热起点的记录完全不参与回放。"""
    key = _make_bucket(10, 60.0)
    _seed_consume(f"rid-old-{_rid()}", T0 - 200, [(key, 999)], True)
    _seed_consume(f"rid-in-{_rid()}", T0 + 1, [(key, 1)], True)
    body = _sim({"from": T0, "to": T0 + 50,
                 "buckets": [{"key": key, "limit": 1, "window_seconds": 60.0}]})
    assert body["transitions"]["allowed_to_allowed"] == 1


# ---------------------------------------------------------------- 滚动过期

def test_rolling_expiry_within_simulation():
    """模拟内部按事件 created_at + window 滚动释放额度。"""
    key = _make_bucket(2, 100.0)
    _seed_consume(f"rid-a-{_rid()}", T0, [(key, 2)], True)
    _seed_consume(f"rid-b-{_rid()}", T0 + 50, [(key, 1)], False)  # 原始：窗口内 2，拒绝
    _seed_consume(f"rid-c-{_rid()}", T0 + 101, [(key, 1)], True)   # rid-a 已过期

    # 候选限额仍为 2、窗口 100：rid-b 在 T0+50 仍被拒 → denied→denied；
    # rid-c 时刻 rid-a 过期，放行 → allowed→allowed
    body = _sim({"from": T0, "to": T0 + 200,
                 "buckets": [{"key": key, "limit": 2, "window_seconds": 100.0}]})
    t = body["transitions"]
    assert t["allowed_to_allowed"] == 2
    assert t["denied_to_denied"] == 1


# ---------------------------------------------------------------- 多桶

def test_multi_bucket_all_or_nothing():
    """一个请求的任一桶超限 → 模拟整体拒绝，且任何桶都不扣减。"""
    k1, k2 = _make_bucket(10, 100.0), _make_bucket(10, 100.0)
    # 预热把 k2 打满到 10
    _seed_consume(f"rid-w-{_rid()}", T0 - 10, [(k2, 10)], True)
    rid = f"rid-m-{_rid()}"
    _seed_consume(rid, T0 + 1, [(k1, 1), (k2, 1)], True)  # 原始宽松时放行
    body = _sim({
        "from": T0, "to": T0 + 50,
        "buckets": [
            {"key": k1, "limit": 10, "window_seconds": 100.0},
            {"key": k2, "limit": 10, "window_seconds": 100.0},
        ],
    })
    assert body["transitions"]["allowed_to_denied"] == 1
    flip = body["flip_details"][0]
    assert flip["request_id"] == rid
    assert sorted(b["key"] for b in flip["buckets"]) == sorted([k1, k2])
    # 整体拒绝：两桶峰值都不应包含该请求（k2=10 来自预热，k1=0 不在列表）
    peaks = {p["key"]: p["peak_used"] for p in body["peak_usage"]}
    assert peaks.get(k2) == 10
    assert k1 not in peaks


def test_unspecified_bucket_uses_current_config():
    """候选未覆盖的桶沿用当前配置，仍正常参与多桶判定。"""
    k1, k2 = _make_bucket(10, 60.0), _make_bucket(10, 60.0)
    rid = f"rid-{_rid()}"
    _seed_consume(rid, T0, [(k1, 1), (k2, 1)], True)
    # 只给 k1 候选（放宽到 100），k2 沿用当前 10；判定不变
    body = _sim({"from": T0, "to": T0 + 60,
                 "buckets": [{"key": k1, "limit": 100, "window_seconds": 60.0}]})
    assert body["transitions"]["allowed_to_allowed"] == 1
    assert body["flip_count"] == 0


# ---------------------------------------------------------------- 排序与截断

def test_flip_order_and_limit_100():
    """翻转明细严格按 (created_at, request_id) 顺序，且最多 100 条。"""
    key = _make_bucket(1000, 1000.0)
    # 120 笔区间内放行；候选限额 0 不可表达，收紧为 1 → 仅第 1 笔放行，
    # 其余 119 笔翻转
    rids = []
    for i in range(120):
        rid = f"zzz-{i:04d}"  # request_id 同刻时参与二级排序
        rids.append(rid)
        _seed_consume(rid, T0, [(key, 1)], True)
    body = _sim({"from": T0, "to": T0 + 50,
                 "buckets": [{"key": key, "limit": 1, "window_seconds": 1000.0}]})
    t = body["transitions"]
    assert t["allowed_to_allowed"] == 1
    assert t["allowed_to_denied"] == 119
    assert body["flip_count"] == 119
    assert len(body["flip_details"]) == 100
    # 截取的是顺序上的前 100 条（第 2..101 笔，request_id 升序）
    flipped_ids = [f["request_id"] for f in body["flip_details"]]
    assert flipped_ids == rids[1:101]
    assert all(f["created_at"] == T0 for f in body["flip_details"])


def test_ordering_by_created_at_then_request_id():
    """同 created_at 多条记录按 request_id 稳定排序。"""
    key = _make_bucket(1, 100.0)
    _seed_consume("b-rid", T0, [(key, 1)], True)
    _seed_consume("a-rid", T0, [(key, 1)], True)
    _seed_consume("c-rid", T0, [(key, 1)], True)
    body = _sim({"from": T0, "to": T0 + 50,
                 "buckets": [{"key": key, "limit": 1, "window_seconds": 100.0}]})
    # a 放行；b、c 拒绝（a 字典序最先）
    assert [f["request_id"] for f in body["flip_details"]] == ["b-rid", "c-rid"]
    t = body["transitions"]
    assert t["allowed_to_allowed"] == 1
    assert t["allowed_to_denied"] == 2


# ---------------------------------------------------------------- 释放与只读

def test_released_consumptions_are_not_replayed():
    """已被 release 的消费不回放：不会虚增模拟用量。"""
    key = _make_bucket(2, 100.0)
    _seed_consume(f"rid-rel-{_rid()}", T0, [(key, 2)], True, released=True)
    rid = f"rid-new-{_rid()}"
    _seed_consume(rid, T0 + 1, [(key, 2)], False)  # 原始固化时可能为拒绝
    # 候选限额 2：已释放事件忽略后，rid-new 应放行 → denied→allowed
    body = _sim({"from": T0, "to": T0 + 50,
                 "buckets": [{"key": key, "limit": 2, "window_seconds": 100.0}]})
    assert body["transitions"]["denied_to_allowed"] == 1
    peak = [p for p in body["peak_usage"] if p["key"] == key][0]
    assert peak["peak_used"] == 2


def test_simulation_is_read_only():
    """模拟不写业务数据、不追加审计链。"""
    from app import audit

    key = _make_bucket(5, 100.0)
    _seed_consume(f"rid-x-{_rid()}", T0, [(key, 5)], True)
    _seed_consume(f"rid-y-{_rid()}", T0 + 1, [(key, 1)], False)

    before = client.get("/v1/audit/verify").json()
    usage_before = client.get(f"/v1/buckets/{key}/usage").json()["used"]

    for _ in range(3):
        body = _sim({"from": T0, "to": T0 + 50,
                     "buckets": [{"key": key, "limit": 5, "window_seconds": 100.0}]})
        # 配置未改变：首笔放行打满 5/5，第二笔原始/模拟均拒绝
        assert body["transitions"]["allowed_to_allowed"] == 1
        assert body["transitions"]["denied_to_denied"] == 1

    after = client.get("/v1/audit/verify").json()
    usage_after = client.get(f"/v1/buckets/{key}/usage").json()["used"]
    assert after["checked_count"] == before["checked_count"]
    assert after["head_hash"] == before["head_hash"]
    assert usage_after == usage_before
    assert after["valid"] is True


def test_half_open_interval_end_exclusive():
    """区间为 [from, to)：created_at == to 的记录不计入。"""
    key = _make_bucket(10, 100.0)
    _seed_consume(f"rid-at-to-{_rid()}", T0 + 50, [(key, 1)], True)
    body = _sim({"from": T0, "to": T0 + 50, "buckets": []})
    assert sum(body["transitions"].values()) == 0
