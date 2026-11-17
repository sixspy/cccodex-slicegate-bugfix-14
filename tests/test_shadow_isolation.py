import threading

from fastapi.testclient import TestClient

from app import app
from conftest import login


def _mk_slice(c, h, no, rows=5, expect=5):
    r = c.post("/api/slices", headers=h, json={
        "slice_no": no, "biz_date": "2026-11-05", "partition_key": "east",
        "row_count": rows, "expected_count": expect})
    assert r.status_code == 200
    return r.json()["id"]


def _events(c, h):
    return [a["event"] for a in c.get("/api/audit", headers=h).json()["items"]]


def test_shadow_never_in_formal_list():
    c = TestClient(app)
    h = login(c)
    sid = _mk_slice(c, h, "SL-ISO-1")
    t = c.post(f"/api/slices/{sid}/trial", headers=h, json={"consumer": "ads_iso"})
    assert t.status_code == 200
    wid = t.json()["shadow_id"]
    formal = c.get("/api/watermarks", headers=h).json()["items"]
    assert formal, "正式集合不应为空"
    assert all(w["kind"] == "正式" for w in formal), "影子水位混入正式展示集合"
    assert not any(w["id"] == wid for w in formal)
    shadow = c.get("/api/watermarks/shadow", headers=h).json()["items"]
    assert any(w["id"] == wid for w in shadow)
    assert all(w["is_current"] == 0 for w in shadow), "影子不得成为现行"
    # 试跑不算正式消费进度：正式集合计数不变
    ov = c.get("/api/watermarks/overview", headers=h).json()
    assert ov["counts"]["formal"] == len(formal)
    assert ov["counts"]["shadow"] == len(shadow)


def test_trial_incomplete_rejected():
    c = TestClient(app)
    h = login(c)
    sid = _mk_slice(c, h, "SL-ISO-2", rows=3, expect=5)  # 部分完整
    t = c.post(f"/api/slices/{sid}/trial", headers=h, json={"consumer": "ads_iso"})
    assert t.status_code == 409


def test_formal_list_rejects_shadow_param():
    c = TestClient(app)
    h = login(c)
    r = c.get("/api/watermarks", headers=h, params={"include_shadow": "1"})
    assert r.status_code == 409
    assert "影子口径偏了" in r.json()["detail"]
    r2 = c.get("/api/watermarks", headers=h, params={"kind": "影子"})
    assert r2.status_code == 409
    assert "影子口径偏了" in _events(c, h)


def test_promote_atomic_single_current():
    c = TestClient(app)
    h = login(c)
    sid = _mk_slice(c, h, "SL-ISO-3")
    wid = c.post(f"/api/slices/{sid}/trial", headers=h, json={"consumer": "ads_p"}).json()["shadow_id"]
    # 先正式放行推进一次，制造既有现行
    sid2 = _mk_slice(c, h, "SL-ISO-4")
    assert c.post(f"/api/slices/{sid2}/release", headers=h, json={"consumer": "ads_p"}).status_code == 200
    p = c.post(f"/api/watermarks/{wid}/promote", headers=h)
    assert p.status_code == 200
    mine = [w for w in c.get("/api/watermarks", headers=h).json()["items"] if w["consumer"] == "ads_p"]
    currents = [w for w in mine if w["is_current"] == 1]
    assert len(currents) == 1, "禁止双现行并存"
    assert currents[0]["id"] == wid and currents[0]["kind"] == "正式"
    assert not any(w["id"] == wid for w in c.get("/api/watermarks/shadow", headers=h).json()["items"])
    assert "影子晋升" in _events(c, h)
    # 重复晋升（已非影子）→ 偏离口径，拒绝并记录
    again = c.post(f"/api/watermarks/{wid}/promote", headers=h)
    assert again.status_code == 409
    assert "影子口径偏了" in again.json()["detail"]
    assert "影子口径偏了" in _events(c, h)


def test_isolation_spec_locked_same_day():
    c = TestClient(app)
    h = login(c)
    spec = c.get("/api/watermarks/isolation", headers=h).json()["spec"]
    assert spec["mode"] == "乙"
    # 换口径（甲）→ 偏离，拒绝并记录
    r = c.post("/api/watermarks/isolation", headers=h, json={"mode": "甲"})
    assert r.status_code == 409
    assert "影子口径偏了" in r.json()["detail"]
    # 当天重写同口径 → 当天不许换
    r2 = c.post("/api/watermarks/isolation", headers=h, json={"mode": "乙"})
    assert r2.status_code == 409
    assert "当天不许换" in r2.json()["detail"]
    assert "影子口径偏了" in _events(c, h)


def test_release_and_promote_concurrent_queue():
    c = TestClient(app)
    h = login(c)
    sid = _mk_slice(c, h, "SL-ISO-5")
    wid = c.post(f"/api/slices/{sid}/trial", headers=h, json={"consumer": "ads_c"}).json()["shadow_id"]
    sid2 = _mk_slice(c, h, "SL-ISO-6")
    barrier = threading.Barrier(2)
    results = {}

    def do_release():
        barrier.wait()
        results["rel"] = c.post(f"/api/slices/{sid2}/release", headers=h, json={"consumer": "ads_c"})

    def do_promote():
        barrier.wait()
        results["pro"] = c.post(f"/api/watermarks/{wid}/promote", headers=h)

    ts = [threading.Thread(target=do_release), threading.Thread(target=do_promote)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    assert results["rel"].status_code == 200
    assert results["pro"].status_code == 200
    # 占坑排队：后手且仅后手标明影子争用
    flags = sorted([results["rel"].json()["contended"], results["pro"].json()["contended"]])
    assert flags == [False, True]
    mine = [w for w in c.get("/api/watermarks", headers=h).json()["items"] if w["consumer"] == "ads_c"]
    assert sum(1 for w in mine if w["is_current"] == 1) == 1, "争用后仍须唯一现行"
    assert sum(w["contended"] for w in mine) == 1
    ev = _events(c, h)
    assert ev.count("影子争用") == 1
    # 审计台与水位台同源：争用计数一致
    ov = c.get("/api/watermarks/overview", headers=h).json()["counts"]
    assert ov["contended"] == 1 and ov["contention"] == 1
