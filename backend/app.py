import hashlib
import secrets
import sqlite3
import threading
import time
import uuid
from datetime import datetime
from pathlib import Path

from fastapi import FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
DB_PATH = DATA / "app.db"
FRONTEND = ROOT / "frontend"
SESSIONS: dict[str, str] = {}

PRODUCT = "日切放行"
SKELETON = "业务日分区切片登记后裁决完整度，完整才放行下游，并与消费水位对账"

# 影子隔离说明：本仓择一贯彻「乙锁定」，写入后当天不许换。
ISO_MODE = "乙"
ISO_TEXT = (
    "乙锁定：影子与正式同表，强制影子标记；默认列表过滤影子；"
    "晋升原子切换标记并禁止双现行并存；说明写入后当天不许换；"
    "偏离说明即拒绝并记「影子口径偏了」。"
)
SLOT_HOLD_SECONDS = 0.2  # 占坑持有窗口：同时提交的晋升与放行必然排队，后手标明影子争用


def connect():
    DATA.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def sha(p: str) -> str:
    return hashlib.sha256(p.encode("utf-8")).hexdigest()


def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def ensure_users(conn):
    conn.execute("""CREATE TABLE IF NOT EXISTS users(
        username TEXT PRIMARY KEY, password_sha TEXT NOT NULL, role TEXT NOT NULL)""")
    if conn.execute("SELECT COUNT(*) c FROM users").fetchone()["c"] == 0:
        conn.execute("INSERT INTO users VALUES (?,?,?)", ("admin", sha("123456"), "管理员"))
        conn.execute("INSERT INTO users VALUES (?,?,?)", ("viewer", sha("view123"), "只读"))


class LoginIn(BaseModel):
    username: str
    password: str


def require_user(authorization: str | None) -> str:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "未登录")
    u = SESSIONS.get(authorization.removeprefix("Bearer ").strip())
    if not u:
        raise HTTPException(401, "登录已失效")
    return u


app = FastAPI(title=PRODUCT)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])


# ================= 水位读写层：影子隔离（乙锁定） =================

def audit(conn, event: str, detail: str, actor: str):
    conn.execute(
        "INSERT INTO wm_audit(event,detail,actor,created_at) VALUES (?,?,?,?)",
        (event, detail, actor, now_iso()),
    )


def deviate(conn, detail: str, actor: str):
    """偏离影子隔离说明：拒绝并记「影子口径偏了」。"""
    audit(conn, "影子口径偏了", detail, actor)
    conn.commit()
    raise HTTPException(409, f"影子口径偏了：{detail}")


class _Slot:
    """单个消费方水位槽位的占坑队列（单机库单进程，FIFO）。"""

    def __init__(self):
        self.cond = threading.Condition()
        self.holder: str | None = None
        self.queue: list[str] = []


_SLOTS: dict[str, _Slot] = {}
_SLOTS_GUARD = threading.Lock()


def _slot_for(consumer: str) -> _Slot:
    with _SLOTS_GUARD:
        s = _SLOTS.get(consumer)
        if s is None:
            s = _SLOTS[consumer] = _Slot()
        return s


def _acquire_slot(consumer: str, op: str) -> tuple[str, bool]:
    """占坑排队：返回 (坑位号, 是否后手)。后手须标明影子争用。"""
    op_id = f"{op}-{uuid.uuid4().hex[:8]}"
    s = _slot_for(consumer)
    with s.cond:
        queued = s.holder is not None or bool(s.queue)
        s.queue.append(op_id)
        while s.holder is not None or s.queue[0] != op_id:
            s.cond.wait()
        s.queue.pop(0)
        s.holder = op_id
    return op_id, queued


def _release_slot(consumer: str, op_id: str):
    s = _slot_for(consumer)
    with s.cond:
        if s.holder == op_id:
            s.holder = None
        s.cond.notify_all()


def migrate_watermarks(conn):
    """老库补列：影子标记/现行标记/争用标记/备注，并补齐每消费方唯一现行。"""
    cols = {r["name"] for r in conn.execute("PRAGMA table_info(watermarks)")}
    for name, ddl in [
        ("kind", "ALTER TABLE watermarks ADD COLUMN kind TEXT NOT NULL DEFAULT '正式'"),
        ("is_current", "ALTER TABLE watermarks ADD COLUMN is_current INTEGER NOT NULL DEFAULT 0"),
        ("contended", "ALTER TABLE watermarks ADD COLUMN contended INTEGER NOT NULL DEFAULT 0"),
        ("note", "ALTER TABLE watermarks ADD COLUMN note TEXT NOT NULL DEFAULT ''"),
    ]:
        if name not in cols:
            conn.execute(ddl)
    for r in conn.execute("SELECT DISTINCT consumer FROM watermarks WHERE kind='正式'").fetchall():
        cons = r["consumer"]
        cur = conn.execute(
            "SELECT COUNT(*) c FROM watermarks WHERE consumer=? AND kind='正式' AND is_current=1", (cons,)
        ).fetchone()["c"]
        if cur == 0:
            latest = conn.execute(
                "SELECT id FROM watermarks WHERE consumer=? AND kind='正式' ORDER BY id DESC LIMIT 1", (cons,)
            ).fetchone()
            if latest:
                conn.execute("UPDATE watermarks SET is_current=1 WHERE id=?", (latest["id"],))


def ensure_spec(conn):
    """影子隔离说明落库：写入后当天不许换。"""
    if conn.execute("SELECT COUNT(*) c FROM isolation_spec").fetchone()["c"] == 0:
        conn.execute(
            "INSERT INTO isolation_spec(mode,spec_date,written_at,text) VALUES (?,?,?,?)",
            (ISO_MODE, datetime.now().date().isoformat(), now_iso(), ISO_TEXT),
        )
        audit(conn, "隔离说明", f"写入影子隔离说明（{ISO_MODE}锁定），当天不许换", "system")


def get_spec(conn):
    row = conn.execute("SELECT * FROM isolation_spec ORDER BY id DESC LIMIT 1").fetchone()
    return dict(row) if row else None


def init_db():
    with connect() as conn:
        ensure_users(conn)
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS slices(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          slice_no TEXT UNIQUE NOT NULL,
          biz_date TEXT NOT NULL,
          partition_key TEXT NOT NULL,
          row_count INTEGER NOT NULL,
          expected_count INTEGER NOT NULL,
          completeness TEXT NOT NULL,
          release_status TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS releases(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          slice_id INTEGER NOT NULL,
          consumer TEXT NOT NULL,
          status TEXT NOT NULL,
          note TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS watermarks(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          consumer TEXT NOT NULL,
          biz_date TEXT NOT NULL,
          watermark TEXT NOT NULL,
          source_slice TEXT NOT NULL,
          kind TEXT NOT NULL DEFAULT '正式',
          is_current INTEGER NOT NULL DEFAULT 0,
          contended INTEGER NOT NULL DEFAULT 0,
          note TEXT NOT NULL DEFAULT ''
        );
        CREATE TABLE IF NOT EXISTS wm_audit(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          event TEXT NOT NULL,
          detail TEXT NOT NULL,
          actor TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS isolation_spec(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          mode TEXT NOT NULL,
          spec_date TEXT NOT NULL,
          written_at TEXT NOT NULL,
          text TEXT NOT NULL
        );
        """)
        migrate_watermarks(conn)
        # 乙锁定硬约束：同一消费方正式水位至多一条现行，禁止双现行并存
        conn.execute("""CREATE UNIQUE INDEX IF NOT EXISTS ux_watermarks_one_current
                        ON watermarks(consumer) WHERE kind='正式' AND is_current=1""")
        ensure_spec(conn)
        if conn.execute("SELECT COUNT(*) c FROM slices").fetchone()["c"] == 0:
            rows = [
                ("SL-2026-11-01-east", "2026-11-01", "east", 980, 1000, "部分完整", "未放行"),
                ("SL-2026-11-01-west", "2026-11-01", "west", 1000, 1000, "完整", "已放行"),
                ("SL-2026-11-02-east", "2026-11-02", "east", 0, 1000, "待检", "未放行"),
            ]
            for r in rows:
                conn.execute("INSERT INTO slices(slice_no,biz_date,partition_key,row_count,expected_count,completeness,release_status) VALUES (?,?,?,?,?,?,?)", r)
            conn.execute("INSERT INTO releases(slice_id,consumer,status,note,created_at) VALUES (2,'ads_order','成功','完整切片放行','2026-11-01T18:00:00')")
            conn.execute("""INSERT INTO watermarks(consumer,biz_date,watermark,source_slice,kind,is_current,contended,note)
                            VALUES ('ads_order','2026-11-01','2026-11-01T18:00:00','SL-2026-11-01-west','正式',1,0,'正式放行推进现行水位')""")
            conn.execute("""INSERT INTO watermarks(consumer,biz_date,watermark,source_slice,kind,is_current,contended,note)
                            VALUES ('ads_order','2026-11-01','2026-11-01T19:30:00','SL-2026-11-01-west','影子',0,0,'试跑影子水位（隔离，不进正式集合）')""")


@app.on_event("startup")
def _start():
    init_db()


@app.get("/api/health")
def health():
    with connect() as c:
        n = c.execute("SELECT COUNT(*) c FROM slices").fetchone()["c"]
    return {"ok": True, "product": PRODUCT, "skeleton": SKELETON, "slices": n, "db": str(DB_PATH), "isolation": ISO_MODE}


@app.post("/api/login")
def login(body: LoginIn):
    with connect() as c:
        row = c.execute("SELECT username, role FROM users WHERE username=? AND password_sha=?", (body.username, sha(body.password))).fetchone()
    if not row:
        raise HTTPException(401, "账号或密码错误")
    tok = secrets.token_hex(16); SESSIONS[tok] = row["username"]
    return {"token": tok, "username": row["username"], "role": row["role"]}


@app.get("/api/slices")
def list_slices(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        rows = c.execute("SELECT * FROM slices ORDER BY id").fetchall()
    return {"items": [dict(r) for r in rows]}


class SliceIn(BaseModel):
    slice_no: str
    biz_date: str
    partition_key: str
    row_count: int
    expected_count: int


@app.post("/api/slices")
def add_slice(body: SliceIn, authorization: str | None = Header(default=None)):
    require_user(authorization)
    comp = "完整" if body.row_count >= body.expected_count and body.expected_count > 0 else ("部分完整" if body.row_count > 0 else "待检")
    with connect() as c:
        try:
            cur = c.execute(
                "INSERT INTO slices(slice_no,biz_date,partition_key,row_count,expected_count,completeness,release_status) VALUES (?,?,?,?,?,?,?)",
                (body.slice_no, body.biz_date, body.partition_key, body.row_count, body.expected_count, comp, "未放行"),
            )
            return {"id": cur.lastrowid, "completeness": comp}
        except sqlite3.IntegrityError:
            raise HTTPException(409, "切片号已存在")


@app.post("/api/slices/{sid}/evaluate")
def evaluate(sid: int, authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        row = c.execute("SELECT * FROM slices WHERE id=?", (sid,)).fetchone()
        if not row:
            raise HTTPException(404, "切片不存在")
        comp = "完整" if row["row_count"] >= row["expected_count"] and row["expected_count"] > 0 else ("部分完整" if row["row_count"] > 0 else "待检")
        c.execute("UPDATE slices SET completeness=? WHERE id=?", (comp, sid))
    return {"id": sid, "completeness": comp}


class ReleaseIn(BaseModel):
    consumer: str


@app.post("/api/slices/{sid}/release")
def release(sid: int, body: ReleaseIn, authorization: str | None = Header(default=None)):
    """正式放行：推进正式水位现行。与影子晋升同槽占坑排队，后手标明影子争用。"""
    user = require_user(authorization)
    with connect() as c:
        row = c.execute("SELECT * FROM slices WHERE id=?", (sid,)).fetchone()
        if not row:
            raise HTTPException(404, "切片不存在")
        if row["completeness"] != "完整":
            c.execute("INSERT INTO releases(slice_id,consumer,status,note,created_at) VALUES (?,?,?,?,?)",
                      (sid, body.consumer, "拒绝", "完整度未达标", now_iso()))
            c.commit()
            raise HTTPException(409, "未完整禁止放行")
        slice_no, biz_date = row["slice_no"], row["biz_date"]
    op_id, queued = _acquire_slot(body.consumer, "正式放行")
    try:
        time.sleep(SLOT_HOLD_SECONDS)
        with connect() as c:
            ts = now_iso()
            c.execute("UPDATE slices SET release_status='已放行' WHERE id=?", (sid,))
            c.execute("INSERT INTO releases(slice_id,consumer,status,note,created_at) VALUES (?,?,?,?,?)",
                      (sid, body.consumer, "成功", "完整切片放行", ts))
            c.execute("UPDATE watermarks SET is_current=0 WHERE consumer=? AND kind='正式' AND is_current=1", (body.consumer,))
            c.execute("""INSERT INTO watermarks(consumer,biz_date,watermark,source_slice,kind,is_current,contended,note)
                         VALUES (?,?,?,?,'正式',1,?,?)""",
                      (body.consumer, biz_date, ts, slice_no, 1 if queued else 0,
                       "正式放行推进现行水位" + ("；影子争用后执行" if queued else "")))
            if queued:
                audit(c, "影子争用", f"正式放行（{slice_no}→{body.consumer}）与影子晋升同时提交，占坑排队后执行", user)
            audit(c, "正式放行", f"{slice_no} 放行给 {body.consumer}，正式水位推进为现行", user)
            c.commit()
    finally:
        _release_slot(body.consumer, op_id)
    return {"ok": True, "contended": queued}


@app.post("/api/slices/{sid}/trial")
def trial(sid: int, body: ReleaseIn, authorization: str | None = Header(default=None)):
    """试跑：只写影子水位（强制影子标记），不进正式展示集合，不算正式消费进度。"""
    user = require_user(authorization)
    with connect() as c:
        row = c.execute("SELECT * FROM slices WHERE id=?", (sid,)).fetchone()
        if not row:
            raise HTTPException(404, "切片不存在")
        if row["completeness"] != "完整":
            audit(c, "影子试跑", f"{row['slice_no']} 试跑被拒：完整度未达标", user)
            c.commit()
            raise HTTPException(409, "未完整禁止试跑")
        cur = c.execute("""INSERT INTO watermarks(consumer,biz_date,watermark,source_slice,kind,is_current,contended,note)
                           VALUES (?,?,?,?,'影子',0,0,'试跑影子水位（隔离，不进正式集合）')""",
                        (body.consumer, row["biz_date"], now_iso(), row["slice_no"]))
        shadow_id = cur.lastrowid
        audit(c, "影子试跑", f"{row['slice_no']} 试跑写入影子水位(id={shadow_id})，隔离不进正式集合", user)
        c.commit()
    return {"ok": True, "shadow_id": shadow_id}


@app.get("/api/releases")
def list_releases(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        rows = c.execute("""SELECT r.*, s.slice_no FROM releases r JOIN slices s ON s.id=r.slice_id ORDER BY r.id DESC""").fetchall()
    return {"items": [dict(r) for r in rows]}


# ---------- 水位读侧：正式集合默认过滤影子 ----------

@app.get("/api/watermarks")
def list_wm(kind: str | None = None, include_shadow: str | None = None, authorization: str | None = Header(default=None)):
    """正式水位展示集合：只含正式行。请求夹带影子即偏离隔离说明，拒绝并记录。"""
    user = require_user(authorization)
    bad_kind = kind not in (None, "", "正式")
    bad_shadow = (include_shadow or "").lower() not in ("", "0", "false", "no")
    if bad_kind or bad_shadow:
        with connect() as c:
            deviate(c, f"正式水位列表请求夹带影子（kind={kind!r}, include_shadow={include_shadow!r}），违反{ISO_MODE}锁定默认过滤", user)
    with connect() as c:
        rows = c.execute("SELECT * FROM watermarks WHERE kind='正式' ORDER BY id DESC").fetchall()
    return {"items": [dict(r) for r in rows]}


@app.get("/api/watermarks/shadow")
def list_shadow(authorization: str | None = Header(default=None)):
    """影子水位隔离区：与正式展示集合分开，绝不混入。"""
    require_user(authorization)
    with connect() as c:
        rows = c.execute("SELECT * FROM watermarks WHERE kind='影子' ORDER BY id DESC").fetchall()
    return {"items": [dict(r) for r in rows]}


@app.get("/api/watermarks/isolation")
def read_isolation(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        spec = get_spec(c)
    return {"spec": spec}


class IsoIn(BaseModel):
    mode: str


@app.post("/api/watermarks/isolation")
def write_isolation(body: IsoIn, authorization: str | None = Header(default=None)):
    """影子隔离说明：择一贯彻。换口径即偏离；当天不许换。"""
    user = require_user(authorization)
    today = datetime.now().date().isoformat()
    with connect() as c:
        spec = get_spec(c)
        if body.mode not in ("甲", "乙"):
            deviate(c, f"隔离说明模式非法：{body.mode!r}", user)
        if spec and body.mode != spec["mode"]:
            deviate(c, f"请求切换为{body.mode}锁定，与已写入的{spec['mode']}锁定不符", user)
        if spec and spec["spec_date"] == today:
            raise HTTPException(409, "影子隔离说明当天不许换")
        c.execute("INSERT INTO isolation_spec(mode,spec_date,written_at,text) VALUES (?,?,?,?)",
                  (ISO_MODE, today, now_iso(), ISO_TEXT))
        audit(c, "隔离说明", f"重申影子隔离说明（{ISO_MODE}锁定）", user)
        c.commit()
    return {"ok": True}


@app.get("/api/watermarks/overview")
def wm_overview(authorization: str | None = Header(default=None)):
    """水位台/审计台同源对账数：两边不得各认各的数。"""
    require_user(authorization)
    with connect() as c:
        spec = get_spec(c)
        g = lambda q, *p: c.execute(q, p).fetchone()["c"]
        counts = {
            "formal": g("SELECT COUNT(*) c FROM watermarks WHERE kind='正式'"),
            "shadow": g("SELECT COUNT(*) c FROM watermarks WHERE kind='影子'"),
            "current": g("SELECT COUNT(*) c FROM watermarks WHERE kind='正式' AND is_current=1"),
            "contended": g("SELECT COUNT(*) c FROM watermarks WHERE contended=1"),
            "audit": g("SELECT COUNT(*) c FROM wm_audit"),
            "deviation": g("SELECT COUNT(*) c FROM wm_audit WHERE event='影子口径偏了'"),
            "contention": g("SELECT COUNT(*) c FROM wm_audit WHERE event='影子争用'"),
        }
    return {"spec": spec, "counts": counts}


@app.post("/api/watermarks/{wid}/promote")
def promote(wid: int, authorization: str | None = Header(default=None)):
    """影子晋升：原子切换标记为正式现行，禁止双现行并存；与正式放行同槽占坑排队。"""
    user = require_user(authorization)
    with connect() as c:
        row = c.execute("SELECT * FROM watermarks WHERE id=?", (wid,)).fetchone()
        if not row:
            raise HTTPException(404, "水位不存在")
        if row["kind"] != "影子":
            deviate(c, f"对正式水位(id={wid})发起晋升，违反{ISO_MODE}锁定：仅影子水位可晋升", user)
        consumer = row["consumer"]
    op_id, queued = _acquire_slot(consumer, "影子晋升")
    try:
        time.sleep(SLOT_HOLD_SECONDS)
        with connect() as c:
            cur = c.execute("SELECT * FROM watermarks WHERE id=?", (wid,)).fetchone()
            if not cur or cur["kind"] != "影子":
                deviate(c, f"水位(id={wid})已非影子，重复晋升违反{ISO_MODE}锁定", user)
            # 原子切换：同一事务内先退旧现行、再升影子为唯一现行，禁止双现行并存
            c.execute("UPDATE watermarks SET is_current=0 WHERE consumer=? AND kind='正式' AND is_current=1", (consumer,))
            r = c.execute("""UPDATE watermarks SET kind='正式', is_current=1, contended=?,
                             note=note||'；影子晋升为现行' WHERE id=? AND kind='影子'""",
                          (1 if queued else 0, wid))
            if r.rowcount != 1:
                deviate(c, f"影子水位(id={wid})晋升落库失败", user)
            if queued:
                audit(c, "影子争用", f"影子晋升（{consumer}，id={wid}）与正式放行同时提交，占坑排队后执行", user)
            audit(c, "影子晋升", f"影子水位(id={wid})原子切换标记，成为 {consumer} 唯一现行正式水位", user)
            c.commit()
    finally:
        _release_slot(consumer, op_id)
    return {"ok": True, "contended": queued}


@app.get("/api/audit")
def list_audit(authorization: str | None = Header(default=None)):
    """审计台流水：口径偏离、影子争用、晋升、放行、试跑全部留痕。"""
    require_user(authorization)
    with connect() as c:
        rows = c.execute("SELECT * FROM wm_audit ORDER BY id DESC").fetchall()
    return {"items": [dict(r) for r in rows]}


@app.get("/")
def index():
    return FileResponse(FRONTEND / "index.html")


app.mount("/static", StaticFiles(directory=str(FRONTEND)), name="static")
