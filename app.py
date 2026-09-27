"""临床试验分层区组随机分配与盲法服务。"""
from __future__ import annotations

import argparse
import hashlib
import json
import random
import sqlite3
import threading
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_DB = BASE_DIR / "randomization.db"
MAX_ARM_LENGTH = 40


class BusinessError(Exception):
    def __init__(self, message, status=400, code="bad_request"):
        super().__init__(message)
        self.message, self.status, self.code = message, status, code


def now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class RandomizationStore:
    def __init__(self, db_path=DEFAULT_DB):
        self.db_path = str(db_path)
        self._lock = threading.Lock()

    def connect(self):
        conn = sqlite3.connect(self.db_path, timeout=15)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=15000")
        return conn

    def init_schema(self):
        with self._lock, self.connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS users(
                    id TEXT PRIMARY KEY, name TEXT NOT NULL,
                    role TEXT NOT NULL CHECK(role IN ('site','coordinator','monitor')),
                    site_id TEXT NOT NULL, active INTEGER NOT NULL DEFAULT 1 CHECK(active IN (0,1))
                );
                CREATE TABLE IF NOT EXISTS trials(
                    id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL UNIQUE,
                    protocol_version TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'draft'
                        CHECK(status IN ('draft','running','stopped')),
                    arms_json TEXT NOT NULL, strata_factors_json TEXT NOT NULL,
                    block_size INTEGER NOT NULL CHECK(block_size >= 2),
                    seed TEXT NOT NULL, created_by TEXT NOT NULL REFERENCES users(id),
                    created_at TEXT NOT NULL, started_at TEXT
                );
                CREATE TABLE IF NOT EXISTS protocol_versions(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    trial_id INTEGER NOT NULL REFERENCES trials(id),
                    version_no INTEGER NOT NULL,
                    protocol_version TEXT NOT NULL,
                    arms_json TEXT NOT NULL, strata_factors_json TEXT NOT NULL,
                    block_size INTEGER NOT NULL CHECK(block_size >= 2),
                    seed TEXT NOT NULL,
                    status TEXT NOT NULL
                        CHECK(status IN ('draft','submitted','active','superseded')),
                    created_by TEXT NOT NULL REFERENCES users(id),
                    created_at TEXT NOT NULL, submitted_at TEXT,
                    approved_by TEXT REFERENCES users(id), approved_at TEXT,
                    UNIQUE(trial_id,version_no),
                    UNIQUE(trial_id,protocol_version)
                );
                CREATE TABLE IF NOT EXISTS strata(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    trial_id INTEGER NOT NULL REFERENCES trials(id),
                    protocol_version_id INTEGER NOT NULL REFERENCES protocol_versions(id),
                    stratum_key TEXT NOT NULL, factors_json TEXT NOT NULL, created_at TEXT NOT NULL,
                    UNIQUE(protocol_version_id,stratum_key)
                );
                CREATE TABLE IF NOT EXISTS allocations(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    trial_id INTEGER NOT NULL REFERENCES trials(id),
                    protocol_version_id INTEGER NOT NULL REFERENCES protocol_versions(id),
                    stratum_id INTEGER NOT NULL REFERENCES strata(id),
                    sequence INTEGER NOT NULL, block_no INTEGER NOT NULL,
                    arm TEXT NOT NULL, used_by INTEGER, used_at TEXT,
                    UNIQUE(stratum_id,sequence)
                );
                CREATE TABLE IF NOT EXISTS participants(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    trial_id INTEGER NOT NULL REFERENCES trials(id),
                    protocol_version_id INTEGER NOT NULL REFERENCES protocol_versions(id),
                    site_id TEXT NOT NULL, external_id TEXT NOT NULL,
                    stratum_id INTEGER NOT NULL REFERENCES strata(id),
                    allocation_id INTEGER NOT NULL UNIQUE REFERENCES allocations(id),
                    allocation_code TEXT NOT NULL UNIQUE, status TEXT NOT NULL DEFAULT 'enrolled'
                        CHECK(status IN ('enrolled','withdrawn','completed')),
                    enrolled_by TEXT NOT NULL REFERENCES users(id), created_at TEXT NOT NULL,
                    UNIQUE(trial_id,external_id)
                );
                CREATE TABLE IF NOT EXISTS unblinding_requests(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    participant_id INTEGER NOT NULL REFERENCES participants(id),
                    requester_id TEXT NOT NULL REFERENCES users(id), reason TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','approved','rejected')),
                    first_approver TEXT REFERENCES users(id), second_approver TEXT REFERENCES users(id),
                    decided_at TEXT, created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS audit_log(
                    id INTEGER PRIMARY KEY AUTOINCREMENT, trial_id INTEGER REFERENCES trials(id),
                    actor_id TEXT NOT NULL REFERENCES users(id), action TEXT NOT NULL,
                    detail TEXT NOT NULL, created_at TEXT NOT NULL
                );
                CREATE UNIQUE INDEX IF NOT EXISTS one_submitted_protocol_version_per_trial
                    ON protocol_versions(trial_id) WHERE status='submitted';
                """
            )
            self._migrate_protocol_versions(conn)

    @staticmethod
    def _columns(conn, table):
        return {row["name"] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}

    @staticmethod
    def _table_exists(conn, table):
        return conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
        ).fetchone() is not None

    def _migrate_protocol_versions(self, conn):
        """Backfill version metadata for databases created before amendments."""
        if not self._table_exists(conn, "protocol_versions"):
            return
        strata_columns = self._columns(conn, "strata")
        allocations_columns = self._columns(conn, "allocations")
        participants_columns = self._columns(conn, "participants")
        if "protocol_version_id" not in strata_columns:
            conn.execute("ALTER TABLE strata ADD COLUMN protocol_version_id INTEGER")
        if "protocol_version_id" not in allocations_columns:
            conn.execute("ALTER TABLE allocations ADD COLUMN protocol_version_id INTEGER")
        if "protocol_version_id" not in participants_columns:
            conn.execute("ALTER TABLE participants ADD COLUMN protocol_version_id INTEGER")

        for trial in conn.execute("SELECT * FROM trials").fetchall():
            row = conn.execute(
                "SELECT id FROM protocol_versions WHERE trial_id=? AND version_no=1",
                (trial["id"],),
            ).fetchone()
            if row:
                protocol_id = row["id"]
            else:
                status = "active" if trial["started_at"] else "draft"
                timestamp = trial["started_at"] or trial["created_at"]
                cur = conn.execute(
                    """INSERT INTO protocol_versions(trial_id,version_no,protocol_version,
                          arms_json,strata_factors_json,block_size,seed,status,
                          created_by,created_at,submitted_at,approved_by,approved_at)
                       VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        trial["id"], 1, trial["protocol_version"],
                        trial["arms_json"], trial["strata_factors_json"],
                        trial["block_size"], trial["seed"], status,
                        trial["created_by"], trial["created_at"],
                        timestamp if status == "active" else None,
                        trial["created_by"] if status == "active" else None,
                        timestamp,
                    ),
                )
                protocol_id = cur.lastrowid
            conn.execute("UPDATE strata SET protocol_version_id=? WHERE protocol_version_id IS NULL AND trial_id=?", (protocol_id, trial["id"]))
            conn.execute("UPDATE allocations SET protocol_version_id=? WHERE protocol_version_id IS NULL AND trial_id=?", (protocol_id, trial["id"]))
            conn.execute("UPDATE participants SET protocol_version_id=? WHERE protocol_version_id IS NULL AND trial_id=?", (protocol_id, trial["id"]))

        # Older databases enforced uniqueness per trial, which would mix old and new
        # random tables. Rebuild legacy tables so uniqueness and foreign keys are scoped
        # to the protocol version while preserving all existing IDs and allocations.
        legacy_strata = any(
            row["name"] == "protocol_version_id" and not row["notnull"]
            for row in conn.execute("PRAGMA table_info(strata)")
        )
        if legacy_strata:
            conn.commit()
            conn.execute("PRAGMA foreign_keys=OFF")
            conn.execute(
                """CREATE TABLE strata_new(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    trial_id INTEGER NOT NULL REFERENCES trials(id),
                    protocol_version_id INTEGER NOT NULL REFERENCES protocol_versions(id),
                    stratum_key TEXT NOT NULL, factors_json TEXT NOT NULL, created_at TEXT NOT NULL,
                    UNIQUE(protocol_version_id,stratum_key)
                )"""
            )
            conn.execute(
                """INSERT INTO strata_new(id,trial_id,protocol_version_id,stratum_key,factors_json,created_at)
                   SELECT id,trial_id,protocol_version_id,stratum_key,factors_json,created_at FROM strata"""
            )
            conn.execute("DROP TABLE strata")
            conn.execute("ALTER TABLE strata_new RENAME TO strata")

            conn.execute(
                """CREATE TABLE allocations_new(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    trial_id INTEGER NOT NULL REFERENCES trials(id),
                    protocol_version_id INTEGER NOT NULL REFERENCES protocol_versions(id),
                    stratum_id INTEGER NOT NULL REFERENCES strata(id),
                    sequence INTEGER NOT NULL, block_no INTEGER NOT NULL,
                    arm TEXT NOT NULL, used_by INTEGER, used_at TEXT,
                    UNIQUE(stratum_id,sequence)
                )"""
            )
            conn.execute(
                """INSERT INTO allocations_new(id,trial_id,protocol_version_id,stratum_id,
                      sequence,block_no,arm,used_by,used_at)
                   SELECT id,trial_id,protocol_version_id,stratum_id,sequence,block_no,arm,used_by,used_at
                   FROM allocations"""
            )
            conn.execute("DROP TABLE allocations")
            conn.execute("ALTER TABLE allocations_new RENAME TO allocations")

            conn.execute(
                """CREATE TABLE participants_new(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    trial_id INTEGER NOT NULL REFERENCES trials(id),
                    protocol_version_id INTEGER NOT NULL REFERENCES protocol_versions(id),
                    site_id TEXT NOT NULL, external_id TEXT NOT NULL,
                    stratum_id INTEGER NOT NULL REFERENCES strata(id),
                    allocation_id INTEGER NOT NULL UNIQUE REFERENCES allocations(id),
                    allocation_code TEXT NOT NULL UNIQUE,
                    status TEXT NOT NULL DEFAULT 'enrolled'
                        CHECK(status IN ('enrolled','withdrawn','completed')),
                    enrolled_by TEXT NOT NULL REFERENCES users(id), created_at TEXT NOT NULL,
                    UNIQUE(trial_id,external_id)
                )"""
            )
            conn.execute(
                """INSERT INTO participants_new(id,trial_id,protocol_version_id,site_id,external_id,
                      stratum_id,allocation_id,allocation_code,status,enrolled_by,created_at)
                   SELECT id,trial_id,protocol_version_id,site_id,external_id,stratum_id,
                          allocation_id,allocation_code,status,enrolled_by,created_at
                   FROM participants"""
            )
            conn.execute("DROP TABLE participants")
            conn.execute("ALTER TABLE participants_new RENAME TO participants")
            violations = conn.execute("PRAGMA foreign_key_check").fetchall()
            if violations:
                conn.execute("PRAGMA foreign_keys=ON")
                raise BusinessError(f"方案版本数据迁移发现外键不一致: {violations[:3]}", 500, "migration_failed")
            conn.execute("PRAGMA foreign_keys=ON")
            conn.commit()

    def seed(self):
        self.init_schema()
        with self.connect() as conn:
            conn.executemany(
                "INSERT OR IGNORE INTO users(id,name,role,site_id) VALUES(?,?,?,?)",
                [
                    ("site1", "中心一协调员", "site", "S001"),
                    ("site2", "中心二协调员", "site", "S002"),
                    ("coord", "项目协调员", "coordinator", "CENTER"),
                    ("monitor1", "独立监查员甲", "monitor", "CENTER"),
                    ("monitor2", "独立监查员乙", "monitor", "CENTER"),
                ],
            )

    def _user(self, conn, user_id, roles=None):
        if not user_id:
            raise BusinessError("缺少 X-User-Id", 401, "authentication_required")
        user = conn.execute("SELECT * FROM users WHERE id=? AND active=1", (user_id,)).fetchone()
        if not user:
            raise BusinessError("用户不存在或已停用", 401, "unknown_user")
        if roles and user["role"] not in roles:
            raise BusinessError("当前角色无权执行此操作", 403, "forbidden")
        return user

    def _trial(self, conn, trial_id):
        row = conn.execute("SELECT * FROM trials WHERE id=?", (trial_id,)).fetchone()
        if not row:
            raise BusinessError("试验不存在", 404, "not_found")
        return row

    def _audit(self, conn, trial_id, actor, action, detail):
        conn.execute(
            "INSERT INTO audit_log(trial_id,actor_id,action,detail,created_at) VALUES(?,?,?,?,?)",
            (trial_id, actor, action, json.dumps(detail, ensure_ascii=False, sort_keys=True), now()),
        )

    @staticmethod
    def _prepare_protocol_config(arms, strata_factors, block_size, seed, allow_empty_strata=False):
        if not isinstance(arms, list) or len(arms) < 2:
            raise BusinessError("至少需要两个试验组", 422, "invalid_arms")
        normalized_arms = [str(a).strip() for a in arms]
        if any(not a or len(a) > MAX_ARM_LENGTH for a in normalized_arms) or len(set(normalized_arms)) != len(normalized_arms):
            raise BusinessError("试验组名称必须非空、唯一且不过长", 422, "invalid_arms")
        if not isinstance(strata_factors, list) or any(not str(x).strip() for x in strata_factors):
            raise BusinessError("分层因素必须是非空数组", 422, "invalid_strata")
        normalized_strata = [str(x).strip() for x in strata_factors]
        if (not allow_empty_strata and not normalized_strata) or len(set(normalized_strata)) != len(normalized_strata):
            raise BusinessError("分层因素必须非空且不重复", 422, "invalid_strata")
        if isinstance(block_size, bool) or not isinstance(block_size, int) or block_size < len(normalized_arms) or block_size % len(normalized_arms) != 0:
            raise BusinessError("区组长度必须为试验组数的正整数倍", 422, "invalid_block_size")
        seed = str(seed or "").strip()
        if len(seed) < 8:
            raise BusinessError("随机种子至少 8 位", 422, "invalid_seed")
        return normalized_arms, normalized_strata, block_size, seed

    @staticmethod
    def _clean_protocol_version(protocol_version):
        version = str(protocol_version or "").strip()
        if not version:
            raise BusinessError("方案版本不能为空", 422, "protocol_version_required")
        return version

    def create_trial(self, user_id, name, protocol_version, arms, strata_factors, block_size, seed):
        name = str(name or "").strip()
        protocol_version = self._clean_protocol_version(protocol_version)
        if len(name) < 3:
            raise BusinessError("试验名称不能为空且至少 3 个字", 422, "invalid_trial")
        arms, strata_factors, block_size, seed = self._prepare_protocol_config(
            arms, strata_factors, block_size, seed
        )
        with self.connect() as conn:
            actor = self._user(conn, user_id, {"coordinator"})
            timestamp = now()
            try:
                cur = conn.execute(
                    """INSERT INTO trials(name,protocol_version,arms_json,strata_factors_json,block_size,seed,created_by,created_at)
                       VALUES(?,?,?,?,?,?,?,?)""",
                    (name, protocol_version, json.dumps(arms, ensure_ascii=False), json.dumps(strata_factors, ensure_ascii=False), block_size, seed, user_id, timestamp),
                )
            except sqlite3.IntegrityError:
                raise BusinessError("试验名称已存在", 409, "trial_exists")
            trial_id = cur.lastrowid
            conn.execute(
                """INSERT INTO protocol_versions(trial_id,version_no,protocol_version,arms_json,
                      strata_factors_json,block_size,seed,status,created_by,created_at)
                   VALUES(?,?,?,?,?,?,?,?,?,?)""",
                (
                    trial_id, 1, protocol_version, json.dumps(arms, ensure_ascii=False),
                    json.dumps(strata_factors, ensure_ascii=False), block_size, seed,
                    "draft", user_id, timestamp,
                ),
            )
            self._audit(conn, trial_id, user_id, "trial.create", {"protocol_version": protocol_version, "arms": len(arms), "block_size": block_size})
            return {"id": trial_id, "name": name, "status": "draft", "protocol_version": protocol_version, "arms": arms, "strata_factors": strata_factors, "block_size": block_size}

    def update_protocol(self, user_id, trial_id, protocol_version, arms=None, strata_factors=None, block_size=None, seed=None):
        """Update a draft trial before activation; enrolled trials need an amendment."""
        protocol_version = self._clean_protocol_version(protocol_version)
        with self.connect() as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._user(conn, user_id, {"coordinator"})
                trial = self._trial(conn, trial_id)
                enrolled = conn.execute("SELECT COUNT(*) FROM participants WHERE trial_id=?", (trial_id,)).fetchone()[0]
                if enrolled or trial["status"] != "draft":
                    raise BusinessError("入组开始后不能直接修改方案，请提交方案修订", 409, "protocol_locked")
                version = conn.execute(
                    "SELECT * FROM protocol_versions WHERE trial_id=? AND status='draft'", (trial_id,)
                ).fetchone()
                if version is None:
                    raise BusinessError("草稿方案不存在或已提交审批", 409, "invalid_protocol_status")
                new_arms = arms if arms is not None else json.loads(version["arms_json"])
                new_strata = strata_factors if strata_factors is not None else json.loads(version["strata_factors_json"])
                new_block = block_size if block_size is not None else version["block_size"]
                new_seed = seed if seed is not None else version["seed"]
                new_arms, new_strata, new_block, new_seed = self._prepare_protocol_config(
                    new_arms, new_strata, new_block, new_seed
                )
                timestamp = now()
                try:
                    conn.execute(
                        """UPDATE trials
                           SET protocol_version=?,arms_json=?,strata_factors_json=?,block_size=?,seed=?
                           WHERE id=?""",
                        (
                            protocol_version, json.dumps(new_arms, ensure_ascii=False),
                            json.dumps(new_strata, ensure_ascii=False), new_block, new_seed, trial_id,
                        ),
                    )
                    conn.execute(
                        """UPDATE protocol_versions
                           SET protocol_version=?,arms_json=?,strata_factors_json=?,block_size=?,seed=?,created_at=?
                           WHERE id=?""",
                        (
                            protocol_version, json.dumps(new_arms, ensure_ascii=False),
                            json.dumps(new_strata, ensure_ascii=False), new_block, new_seed,
                            timestamp, version["id"],
                        ),
                    )
                except sqlite3.IntegrityError:
                    raise BusinessError("方案版本已存在", 409, "protocol_version_exists")
                self._audit(conn, trial_id, user_id, "protocol.update", {"protocol_version": protocol_version})
                return {
                    "id": trial_id, "protocol_version_id": version["id"],
                    "version_no": version["version_no"], "protocol_version": protocol_version,
                    "status": "draft", "arms": new_arms, "strata_factors": new_strata,
                    "block_size": new_block,
                }
            except Exception:
                conn.rollback()
                raise

    @staticmethod
    def create_trial_validation_only(arms, strata_factors, block_size, seed):
        RandomizationStore._prepare_protocol_config(
            arms, strata_factors, block_size, seed, allow_empty_strata=True
        )

    def submit_protocol_amendment(
        self, user_id, trial_id, protocol_version, arms, strata_factors, block_size, seed
    ):
        protocol_version = self._clean_protocol_version(protocol_version)
        arms, strata_factors, block_size, seed = self._prepare_protocol_config(
            arms, strata_factors, block_size, seed
        )
        with self.connect() as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._user(conn, user_id, {"coordinator"})
                trial = self._trial(conn, trial_id)
                if trial["status"] != "running":
                    raise BusinessError("只有入组中的试验需要提交方案修订", 409, "invalid_status")
                pending = conn.execute(
                    "SELECT id FROM protocol_versions WHERE trial_id=? AND status='submitted'",
                    (trial_id,),
                ).fetchone()
                if pending:
                    raise BusinessError("已有待监查员确认的方案修订", 409, "amendment_pending")
                version_no = conn.execute(
                    "SELECT COALESCE(MAX(version_no),0)+1 AS next_no FROM protocol_versions WHERE trial_id=?",
                    (trial_id,),
                ).fetchone()["next_no"]
                timestamp = now()
                try:
                    cur = conn.execute(
                        """INSERT INTO protocol_versions(trial_id,version_no,protocol_version,
                              arms_json,strata_factors_json,block_size,seed,status,
                              created_by,created_at,submitted_at)
                           VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                        (
                            trial_id, version_no, protocol_version,
                            json.dumps(arms, ensure_ascii=False),
                            json.dumps(strata_factors, ensure_ascii=False),
                            block_size, seed, "submitted", user_id, timestamp, timestamp,
                        ),
                    )
                except sqlite3.IntegrityError:
                    raise BusinessError("方案版本已存在或已有待审批修订", 409, "protocol_version_exists")
                self._audit(
                    conn, trial_id, user_id, "protocol.amendment.submit",
                    {
                        "protocol_version_id": cur.lastrowid, "version_no": version_no,
                        "protocol_version": protocol_version, "arms": arms,
                        "strata_factors": strata_factors, "block_size": block_size,
                    },
                )
                return {
                    "id": cur.lastrowid, "trial_id": trial_id, "version_no": version_no,
                    "protocol_version": protocol_version, "status": "submitted",
                    "arms": arms, "strata_factors": strata_factors, "block_size": block_size,
                }
            except Exception:
                conn.rollback()
                raise

    def approve_protocol_amendment(self, user_id, trial_id, amendment_id):
        with self.connect() as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._user(conn, user_id, {"monitor"})
                trial = self._trial(conn, trial_id)
                amendment = conn.execute(
                    "SELECT * FROM protocol_versions WHERE id=? AND trial_id=?",
                    (amendment_id, trial_id),
                ).fetchone()
                if not amendment:
                    raise BusinessError("方案修订不存在", 404, "not_found")
                if amendment["status"] != "submitted":
                    raise BusinessError("该方案修订不是待确认状态", 409, "amendment_not_pending")
                pending_unblinding = conn.execute(
                    """SELECT 1
                       FROM unblinding_requests ur
                       JOIN participants p ON p.id=ur.participant_id
                       WHERE p.trial_id=? AND ur.status='pending'
                       LIMIT 1""",
                    (trial_id,),
                ).fetchone()
                if pending_unblinding:
                    raise BusinessError("存在待审批揭盲申请时不能批准方案修订", 409, "pending_unblinding")
                active = conn.execute(
                    "SELECT * FROM protocol_versions WHERE trial_id=? AND status='active'",
                    (trial_id,),
                ).fetchone()
                if active is None:
                    raise BusinessError("当前试验没有生效中的方案版本", 409, "no_active_protocol")
                timestamp = now()
                arms = json.loads(amendment["arms_json"])
                strata_factors = json.loads(amendment["strata_factors_json"])
                conn.execute(
                    "UPDATE protocol_versions SET status='superseded' WHERE id=?",
                    (active["id"],),
                )
                conn.execute(
                    """UPDATE protocol_versions
                       SET status='active',approved_by=?,approved_at=?
                       WHERE id=?""",
                    (user_id, timestamp, amendment_id),
                )
                conn.execute(
                    """UPDATE trials
                       SET protocol_version=?,arms_json=?,strata_factors_json=?,block_size=?,seed=?
                       WHERE id=?""",
                    (
                        amendment["protocol_version"], amendment["arms_json"],
                        amendment["strata_factors_json"], amendment["block_size"],
                        amendment["seed"], trial_id,
                    ),
                )
                self._audit(
                    conn, trial_id, user_id, "protocol.amendment.approve",
                    {
                        "protocol_version_id": amendment_id,
                        "version_no": amendment["version_no"],
                        "protocol_version": amendment["protocol_version"],
                        "previous_protocol_version_id": active["id"],
                        "previous_protocol_version": active["protocol_version"],
                    },
                )
                return {
                    "id": amendment_id, "trial_id": trial_id,
                    "version_no": amendment["version_no"],
                    "protocol_version": amendment["protocol_version"],
                    "status": "active", "arms": arms,
                    "strata_factors": strata_factors,
                    "block_size": amendment["block_size"],
                    "approved_by": user_id, "approved_at": timestamp,
                }
            except Exception:
                conn.rollback()
                raise

    def start_trial(self, user_id, trial_id):
        with self.connect() as conn:
            self._user(conn, user_id, {"coordinator"})
            trial = self._trial(conn, trial_id)
            if trial["status"] != "draft":
                raise BusinessError("只有草稿试验可以开始", 409, "invalid_status")
            submitted = conn.execute(
                "SELECT id FROM protocol_versions WHERE trial_id=? AND status='submitted'",
                (trial_id,),
            ).fetchone()
            if submitted:
                raise BusinessError("草稿试验有待确认的方案修订，不能开始入组", 409, "amendment_pending")
            timestamp = now()
            conn.execute("UPDATE trials SET status='running',started_at=? WHERE id=?", (timestamp, trial_id))
            conn.execute(
                "UPDATE protocol_versions SET status='active',submitted_at=?,approved_by=?,approved_at=? WHERE trial_id=? AND status='draft'",
                (timestamp, user_id, timestamp, trial_id),
            )
            self._audit(conn, trial_id, user_id, "trial.start", {})
            return {"id": trial_id, "status": "running"}

    def _active_protocol(self, conn, trial):
        protocol = conn.execute(
            "SELECT * FROM protocol_versions WHERE trial_id=? AND status='active'",
            (trial["id"],),
        ).fetchone()
        if protocol is None:
            raise BusinessError("当前试验没有生效中的方案版本", 409, "no_active_protocol")
        return protocol

    def _stratum(self, conn, protocol, factors, site_id):
        trial_id = protocol["trial_id"]
        expected = json.loads(protocol["strata_factors_json"])
        if set(factors) != set(expected):
            raise BusinessError(f"必须提供分层因素: {', '.join(expected)}", 422, "invalid_factors")
        normalized = {k: str(factors[k]).strip() for k in sorted(expected)}
        if any(not v for v in normalized.values()):
            raise BusinessError("分层因素值不能为空", 422, "invalid_factors")
        key = f"{site_id}|" + json.dumps(normalized, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        row = conn.execute(
            "SELECT * FROM strata WHERE protocol_version_id=? AND stratum_key=?",
            (protocol["id"], key),
        ).fetchone()
        if row:
            return row
        cur = conn.execute(
            "INSERT INTO strata(trial_id,protocol_version_id,stratum_key,factors_json,created_at) VALUES(?,?,?,?,?)",
            (trial_id, protocol["id"], key, json.dumps({"site_id": site_id, **normalized}, ensure_ascii=False, sort_keys=True), now()),
        )
        return conn.execute("SELECT * FROM strata WHERE id=?", (cur.lastrowid,)).fetchone()

    def _next_allocation(self, conn, protocol, stratum):
        for block_no in range(1, 101):
            count = conn.execute(
                "SELECT COUNT(*) FROM allocations WHERE stratum_id=? AND block_no=?", (stratum["id"], block_no)
            ).fetchone()[0]
            if count == 0:
                rng = random.Random(f"{protocol['seed']}:{stratum['stratum_key']}:{block_no}")
                arms = json.loads(protocol["arms_json"])
                plan = []
                block_cycles = protocol["block_size"] // len(arms)
                for _ in range(block_cycles):
                    plan.extend(arms)
                rng.shuffle(plan)
                start = conn.execute(
                    "SELECT COALESCE(MAX(sequence),0) FROM allocations WHERE stratum_id=?", (stratum["id"],)
                ).fetchone()[0]
                for offset, arm in enumerate(plan, 1):
                    conn.execute(
                        "INSERT INTO allocations(trial_id,protocol_version_id,stratum_id,sequence,block_no,arm) VALUES(?,?,?,?,?,?)",
                        (protocol["trial_id"], protocol["id"], stratum["id"], start + offset, block_no, arm),
                    )
            free = conn.execute(
                "SELECT * FROM allocations WHERE stratum_id=? AND used_by IS NULL ORDER BY sequence LIMIT 1", (stratum["id"],)
            ).fetchone()
            if free:
                return free
        raise BusinessError("当前方案随机分配表已耗尽，请由统计人员扩展方案", 409, "allocation_exhausted")

    def enroll(self, user_id, trial_id, external_id, factors):
        external_id = str(external_id).strip()
        if not external_id:
            raise BusinessError("外部受试者编号不能为空", 422, "invalid_external_id")
        with self.connect() as conn:
            actor = self._user(conn, user_id, {"site"})
            try:
                conn.execute("BEGIN IMMEDIATE")
                trial = self._trial(conn, trial_id)
                if trial["status"] != "running":
                    raise BusinessError("试验尚未开始或已经停止", 409, "trial_not_running")
                protocol = self._active_protocol(conn, trial)
                existing = conn.execute(
                    "SELECT * FROM participants WHERE trial_id=? AND external_id=?", (trial_id, external_id)
                ).fetchone()
                if existing:
                    if existing["site_id"] != actor["site_id"]:
                        raise BusinessError("不能在当前中心查看其他中心的受试者", 403, "site_isolation")
                    conn.commit()
                    return self._blinded_participant(conn, existing, actor, allow_arm=False, idempotent=True)
                stratum = self._stratum(conn, protocol, factors, actor["site_id"])
                allocation = self._next_allocation(conn, protocol, stratum)
                allocation_code = hashlib.sha256(f"{trial_id}:{external_id}".encode()).hexdigest()[:12].upper()
                cur = conn.execute(
                    """INSERT INTO participants(trial_id,protocol_version_id,site_id,external_id,stratum_id,
                          allocation_id,allocation_code,enrolled_by,created_at)
                       VALUES(?,?,?,?,?,?,?,?,?)""",
                    (trial_id, protocol["id"], actor["site_id"], external_id, stratum["id"], allocation["id"], allocation_code, user_id, now()),
                )
                participant_id = cur.lastrowid
                conn.execute("UPDATE allocations SET used_by=?,used_at=? WHERE id=?", (participant_id, now(), allocation["id"]))
                self._audit(conn, trial_id, user_id, "participant.enroll", {"participant_id": participant_id, "external_id": external_id, "allocation_id": allocation["id"], "protocol_version_id": protocol["id"], "protocol_version": protocol["protocol_version"], "site_id": actor["site_id"]})
                participant = self._fetch_participant(conn, participant_id)
                return self._blinded_participant(conn, participant, actor, allow_arm=False, idempotent=False)
            except sqlite3.IntegrityError as exc:
                conn.rollback()
                if "participants.trial_id, participants.external_id" in str(exc):
                    with self.connect() as retry:
                        row = retry.execute("SELECT * FROM participants WHERE trial_id=? AND external_id=?", (trial_id, external_id)).fetchone()
                        if row and row["site_id"] == actor["site_id"]:
                            return self._blinded_participant(retry, row, actor, False, True)
                raise BusinessError("并发入组冲突，请重新提交", 409, "enrollment_conflict")
            except Exception:
                conn.rollback()
                raise

    def _version_payload(self, conn, protocol_version_id):
        version = conn.execute(
            "SELECT * FROM protocol_versions WHERE id=?", (protocol_version_id,)
        ).fetchone()
        if version is None:
            return None
        return {
            "protocol_version_id": version["id"],
            "version_no": version["version_no"],
            "protocol_version": version["protocol_version"],
            "protocol_status": version["status"],
            "arms": json.loads(version["arms_json"]),
            "strata_factors": json.loads(version["strata_factors_json"]),
            "block_size": version["block_size"],
        }

    def _blinded_participant(self, conn, participant, viewer, allow_arm=False, idempotent=False):
        result = {
            "id": participant["id"], "trial_id": participant["trial_id"],
            "external_id": participant["external_id"], "site_id": participant["site_id"],
            "allocation_code": participant["allocation_code"], "status": participant["status"],
            "created_at": participant["created_at"], "idempotent": idempotent,
        }
        result.update(self._version_payload(conn, participant["protocol_version_id"]) or {})
        if allow_arm:
            result["arm"] = conn.execute("SELECT arm FROM allocations WHERE id=?", (participant["allocation_id"],)).fetchone()["arm"]
        return result

    def list_participants(self, user_id, trial_id):
        with self.connect() as conn:
            actor = self._user(conn, user_id, {"site", "coordinator", "monitor"})
            self._trial(conn, trial_id)
            if actor["role"] == "site":
                rows = conn.execute("SELECT * FROM participants WHERE trial_id=? AND site_id=? ORDER BY id", (trial_id, actor["site_id"])).fetchall()
            else:
                rows = conn.execute("SELECT * FROM participants WHERE trial_id=? ORDER BY id", (trial_id,)).fetchall()
            return [self._blinded_participant(conn, row, actor) for row in rows]

    def get_participant(self, user_id, participant_id):
        with self.connect() as conn:
            actor = self._user(conn, user_id, {"site", "coordinator", "monitor"})
            row = self._fetch_participant(conn, participant_id)
            if not row:
                raise BusinessError("受试者不存在", 404, "not_found")
            if actor["role"] == "site" and row["site_id"] != actor["site_id"]:
                raise BusinessError("只能查看本中心受试者", 403, "site_isolation")
            approved = conn.execute(
                "SELECT 1 FROM unblinding_requests WHERE participant_id=? AND status='approved'", (participant_id,)
            ).fetchone() is not None
            return self._blinded_participant(conn, row, actor, allow_arm=approved)

    def _fetch_participant(self, conn, participant_id):
        return conn.execute("SELECT * FROM participants WHERE id=?", (participant_id,)).fetchone()

    def request_unblinding(self, user_id, participant_id, reason):
        if len(reason.strip()) < 8:
            raise BusinessError("揭盲原因至少 8 字", 422, "reason_required")
        with self.connect() as conn:
            actor = self._user(conn, user_id, {"site", "coordinator"})
            participant = self._fetch_participant(conn, participant_id)
            if not participant:
                raise BusinessError("受试者不存在", 404, "not_found")
            if actor["role"] == "site" and participant["site_id"] != actor["site_id"]:
                raise BusinessError("不能申请其他中心的揭盲", 403, "site_isolation")
            open_request = conn.execute(
                "SELECT id FROM unblinding_requests WHERE participant_id=? AND status='pending'", (participant_id,)
            ).fetchone()
            if open_request:
                raise BusinessError("该受试者已有待审批的揭盲申请", 409, "request_exists")
            cur = conn.execute(
                "INSERT INTO unblinding_requests(participant_id,requester_id,reason,created_at) VALUES(?,?,?,?)",
                (participant_id, user_id, reason.strip(), now()),
            )
            self._audit(conn, participant["trial_id"], user_id, "unblinding.request", {"request_id": cur.lastrowid, "participant_id": participant_id})
            return {"id": cur.lastrowid, "status": "pending"}

    def approve_unblinding(self, user_id, request_id):
        with self.connect() as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                approver = self._user(conn, user_id, {"monitor", "coordinator"})
                request = conn.execute("SELECT * FROM unblinding_requests WHERE id=?", (request_id,)).fetchone()
                if not request:
                    raise BusinessError("揭盲申请不存在", 404, "not_found")
                if request["status"] != "pending":
                    raise BusinessError("揭盲申请已经完成", 409, "already_decided")
                if request["first_approver"] is None:
                    conn.execute("UPDATE unblinding_requests SET first_approver=? WHERE id=?", (user_id, request_id))
                    participant = conn.execute("SELECT * FROM participants WHERE id=?", (request["participant_id"],)).fetchone()
                    self._audit(conn, participant["trial_id"], user_id, "unblinding.approve.first", {"request_id": request_id})
                    return {"id": request_id, "status": "pending", "first_approver": user_id, "second_approval_required": True}
                if request["first_approver"] == user_id:
                    raise BusinessError("两次揭盲审批必须由不同人员完成", 409, "distinct_approver_required")
                conn.execute(
                    "UPDATE unblinding_requests SET second_approver=?,status='approved',decided_at=? WHERE id=?",
                    (user_id, now(), request_id),
                )
                participant = conn.execute("SELECT * FROM participants WHERE id=?", (request["participant_id"],)).fetchone()
                arm = conn.execute("SELECT arm FROM allocations WHERE id=?", (participant["allocation_id"],)).fetchone()["arm"]
                self._audit(conn, participant["trial_id"], user_id, "unblinding.approve.second", {"request_id": request_id, "participant_id": participant["id"]})
                return {"id": request_id, "status": "approved", "first_approver": request["first_approver"], "second_approver": user_id, "arm": arm}
            except Exception:
                conn.rollback()
                raise

    def trial_summary(self, user_id, trial_id):
        with self.connect() as conn:
            actor = self._user(conn, user_id, {"site", "coordinator", "monitor"})
            trial = self._trial(conn, trial_id)
            where, params = "", [trial_id]
            if actor["role"] == "site":
                where, params = " AND site_id=?", [trial_id, actor["site_id"]]
            total = conn.execute(f"SELECT COUNT(*) FROM participants WHERE trial_id=?" + where, params).fetchone()[0]
            by_site = conn.execute(
                f"SELECT site_id,COUNT(*) AS count FROM participants WHERE trial_id=?" + where + " GROUP BY site_id", params
            ).fetchall()
            by_version_rows = conn.execute(
                f"""SELECT pv.id AS protocol_version_id,pv.version_no,pv.protocol_version,
                          pv.status,COUNT(p.id) AS count
                   FROM protocol_versions pv
                   LEFT JOIN participants p
                       ON p.protocol_version_id=pv.id
                       AND p.trial_id=?{where.replace('site_id', 'p.site_id')}
                   WHERE pv.trial_id=?
                   GROUP BY pv.id
                   ORDER BY pv.version_no""",
                params + [trial_id],
            ).fetchall()
            participant_rows = conn.execute(
                f"""SELECT p.*,pv.version_no,pv.protocol_version AS participant_protocol_version,pv.status AS protocol_status
                    FROM participants p
                    JOIN protocol_versions pv ON pv.id=p.protocol_version_id
                    WHERE p.trial_id=?{where.replace('site_id', 'p.site_id')}
                    ORDER BY p.id""",
                params,
            ).fetchall()
            versions = conn.execute(
                """SELECT id,version_no,protocol_version,status,arms_json,strata_factors_json,
                          block_size,created_by,created_at,submitted_at,approved_by,approved_at
                   FROM protocol_versions WHERE trial_id=? ORDER BY version_no""",
                (trial_id,),
            ).fetchall()
            audit = conn.execute("SELECT * FROM audit_log WHERE trial_id=? ORDER BY id", (trial_id,)).fetchall()
            return {
                "trial": {"id": trial["id"], "name": trial["name"], "protocol_version": trial["protocol_version"], "status": trial["status"]},
                "protocol_versions": [
                    {
                        "id": x["id"],
                        "version_no": x["version_no"],
                        "protocol_version": x["protocol_version"],
                        "status": x["status"],
                        "arms": json.loads(x["arms_json"]),
                        "strata_factors": json.loads(x["strata_factors_json"]),
                        "block_size": x["block_size"],
                        "created_by": x["created_by"],
                        "created_at": x["created_at"],
                        "submitted_at": x["submitted_at"],
                        "approved_by": x["approved_by"],
                        "approved_at": x["approved_at"],
                    }
                    for x in versions
                ],
                "participants_visible": total,
                "by_site": [dict(x) for x in by_site],
                "by_protocol_version": [dict(x) for x in by_version_rows],
                "participants": [
                    {
                        "id": x["id"],
                        "external_id": x["external_id"],
                        "site_id": x["site_id"],
                        "allocation_code": x["allocation_code"],
                        "status": x["status"],
                        "created_at": x["created_at"],
                        "protocol_version_id": x["protocol_version_id"],
                        "version_no": x["version_no"],
                        "protocol_version": x["participant_protocol_version"],
                        "protocol_status": x["protocol_status"],
                    }
                    for x in participant_rows
                ],
                "audit": [dict(x) | {"detail": json.loads(x["detail"])} for x in audit],
            }


class Handler(BaseHTTPRequestHandler):
    server_version = "Randomization/1.0"
    def _store(self): return self.server.store  # type: ignore[attr-defined]
    def _body(self):
        length = int(self.headers.get("Content-Length", "0"))
        try: data = json.loads(self.rfile.read(length) or b"{}")
        except (json.JSONDecodeError, UnicodeDecodeError): raise BusinessError("请求体必须是合法 JSON", 400, "invalid_json")
        if not isinstance(data, dict): raise BusinessError("JSON 顶层必须是对象", 422, "invalid_json")
        return data
    def _send(self, status, payload):
        body = json.dumps(payload, ensure_ascii=False).encode()
        self.send_response(status); self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body))); self.end_headers(); self.wfile.write(body)
    def _dispatch(self, method):
        raw_path = urlparse(self.path).path
        path = "/" if raw_path == "/" else raw_path.rstrip("/")
        parts = [p for p in path.split("/") if p]
        user = self.headers.get("X-User-Id", ""); store = self._store()
        if method == "GET" and path == "/":
            body = (BASE_DIR / "web" / "index.html").read_bytes(); self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8"); self.send_header("Content-Length", str(len(body)))
            self.end_headers(); self.wfile.write(body); return
        if method == "GET" and path == "/health": return self._send(200, {"ok": True})
        if parts == ["api", "trials"] and method == "POST":
            d=self._body(); return self._send(201, store.create_trial(user,d.get("name",""),d.get("protocol_version",""),d.get("arms"),d.get("strata_factors"),d.get("block_size"),d.get("seed","")))
        if len(parts) >= 3 and parts[:2] == ["api", "trials"]:
            trial_id=int(parts[2])
            if len(parts)==4 and parts[3]=="protocol" and method=="POST":
                d=self._body(); return self._send(200, store.update_protocol(user,trial_id,d.get("protocol_version",""),d.get("arms"),d.get("strata_factors"),d.get("block_size"),d.get("seed")))
            if len(parts)==4 and parts[3]=="protocol-amendments" and method=="POST":
                d=self._body(); return self._send(201, store.submit_protocol_amendment(user,trial_id,d.get("protocol_version",""),d.get("arms"),d.get("strata_factors"),d.get("block_size"),d.get("seed")))
            if len(parts)==5 and parts[3]=="protocol-amendments" and parts[4].isdigit() and method=="POST":
                return self._send(200, store.approve_protocol_amendment(user,trial_id,int(parts[4])))
            if len(parts)==4 and parts[3]=="start" and method=="POST": return self._send(200, store.start_trial(user,trial_id))
            if len(parts)==4 and parts[3]=="participants" and method=="GET": return self._send(200, {"items": store.list_participants(user,trial_id)})
            if len(parts)==4 and parts[3]=="enroll" and method=="POST":
                d=self._body(); return self._send(201, store.enroll(user,trial_id,d.get("external_id",""),d.get("factors",{})))
            if len(parts)==4 and parts[3]=="summary" and method=="GET": return self._send(200, store.trial_summary(user,trial_id))
        if len(parts)==3 and parts[:2]==["api","participants"] and method=="GET": return self._send(200, store.get_participant(user,int(parts[2])))
        if len(parts)==4 and parts[:2]==["api","participants"] and parts[3]=="unblinding-requests" and method=="POST":
            d=self._body(); return self._send(201, store.request_unblinding(user,int(parts[2]),d.get("reason","")))
        if len(parts)==4 and parts[:2]==["api","unblinding-requests"] and parts[3]=="approve" and method=="POST":
            return self._send(200, store.approve_unblinding(user,int(parts[2])))
        raise BusinessError("接口不存在",404,"not_found")
    def _handle(self, method):
        try: self._dispatch(method)
        except BusinessError as exc: self._send(exc.status,{"error":{"code":exc.code,"message":exc.message}})
        except (ValueError,TypeError): self._send(400,{"error":{"code":"invalid_path","message":"路径参数格式错误"}})
        except Exception as exc: self._send(500,{"error":{"code":"internal_error","message":str(exc)}})
    def do_GET(self): self._handle("GET")
    def do_POST(self): self._handle("POST")
    def log_message(self, fmt, *args): print(f"{self.address_string()} - {fmt % args}")


class RandomizationServer(ThreadingHTTPServer):
    daemon_threads = True
    def __init__(self, address, store): self.store=store; super().__init__(address,Handler)


def main():
    parser=argparse.ArgumentParser(description="临床试验随机分配与盲法服务")
    parser.add_argument("--db",default=str(DEFAULT_DB)); parser.add_argument("--port",type=int,default=8104)
    parser.add_argument("--init",action="store_true"); parser.add_argument("--seed",action="store_true")
    args=parser.parse_args(); store=RandomizationStore(args.db); store.init_schema()
    if args.seed: store.seed()
    if args.init or args.seed: print(f"数据库已初始化: {args.db}"); return
    server=RandomizationServer(("127.0.0.1",args.port),store); print(f"随机化服务运行于 http://127.0.0.1:{args.port}")
    try: server.serve_forever()
    except KeyboardInterrupt: pass
    finally: server.server_close()

if __name__=="__main__": main()
