import sqlite3
import tempfile
import unittest
from collections import Counter
from pathlib import Path

from app import BusinessError, RandomizationStore


class RandomizationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = RandomizationStore(Path(self.tmp.name) / "test.db")
        self.store.seed()
        self.trial = self.store.create_trial(
            "coord", "多中心降压研究", "v1.0", ["A", "B"], ["risk"], 4, "seed-2026-001"
        )
        self.store.start_trial("coord", self.trial["id"])

    def tearDown(self):
        self.tmp.cleanup()

    def test_stratified_block_randomization_and_two_person_unblinding(self):
        participants = [
            self.store.enroll("site1", self.trial["id"], f"S001-{i:03d}", {"risk": "low"})
            for i in range(1, 5)
        ]
        self.assertNotIn("arm", participants[0])
        with self.store.connect() as conn:
            arms = [r["arm"] for r in conn.execute(
                "SELECT a.arm FROM allocations a JOIN participants p ON p.allocation_id=a.id WHERE p.trial_id=? ORDER BY p.id",
                (self.trial["id"],),
            ).fetchall()]
        self.assertEqual(Counter(arms), Counter({"A": 2, "B": 2}))
        request = self.store.request_unblinding("site1", participants[0]["id"], "受试者发生严重不良事件需要紧急处理")
        first = self.store.approve_unblinding("monitor1", request["id"])
        self.assertEqual(first["status"], "pending")
        with self.assertRaises(BusinessError) as ctx:
            self.store.approve_unblinding("monitor1", request["id"])
        self.assertEqual(ctx.exception.code, "distinct_approver_required")
        second = self.store.approve_unblinding("monitor2", request["id"])
        self.assertEqual(second["status"], "approved")
        self.assertIn(second["arm"], {"A", "B"})

    def test_idempotent_enrollment_site_isolation_and_protocol_lock(self):
        first = self.store.enroll("site1", self.trial["id"], "S001-001", {"risk": "high"})
        again = self.store.enroll("site1", self.trial["id"], "S001-001", {"risk": "high"})
        self.assertEqual(first["id"], again["id"])
        self.assertTrue(again["idempotent"])
        with self.store.connect() as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM participants").fetchone()[0], 1)
        with self.assertRaises(BusinessError) as ctx:
            self.store.get_participant("site2", first["id"])
        self.assertEqual(ctx.exception.code, "site_isolation")
        with self.assertRaises(BusinessError) as ctx:
            self.store.update_protocol("coord", self.trial["id"], "v2")
        self.assertEqual(ctx.exception.code, "protocol_locked")

    def test_monitor_approved_amendment_preserves_old_enrollments(self):
        for i in range(1, 3):
            participant = self.store.enroll(
                "site1", self.trial["id"], f"S001-{i:03d}", {"risk": "low"}
            )
            self.assertEqual(participant["protocol_version"], "v1.0")

        with self.store.connect() as conn:
            old_arms = [
                row["arm"]
                for row in conn.execute(
                    """SELECT a.arm FROM participants p
                       JOIN allocations a ON a.id=p.allocation_id
                       WHERE p.trial_id=? ORDER BY p.id""",
                    (self.trial["id"],),
                ).fetchall()
            ]
            old_remaining = conn.execute(
                """SELECT COUNT(*) FROM allocations a
                   JOIN protocol_versions pv ON pv.id=a.protocol_version_id
                   WHERE pv.trial_id=? AND pv.protocol_version='v1.0' AND a.used_by IS NULL""",
                (self.trial["id"],),
            ).fetchone()[0]

        amendment = self.store.submit_protocol_amendment(
            "coord", self.trial["id"], "v2.0", ["A", "B", "C"],
            ["risk", "age"], 6, "seed-2026-002"
        )
        self.assertEqual(amendment["status"], "submitted")
        with self.assertRaises(BusinessError) as ctx:
            self.store.submit_protocol_amendment(
                "coord", self.trial["id"], "v3.0", ["A", "B"], ["risk"], 4, "seed-2026-003"
            )
        self.assertEqual(ctx.exception.code, "amendment_pending")
        with self.assertRaises(BusinessError) as ctx:
            self.store.approve_protocol_amendment("coord", self.trial["id"], amendment["id"])
        self.assertEqual(ctx.exception.code, "forbidden")

        approved = self.store.approve_protocol_amendment(
            "monitor1", self.trial["id"], amendment["id"]
        )
        self.assertEqual(approved["status"], "active")

        new_participant = self.store.enroll(
            "site1", self.trial["id"], "S001-003", {"risk": "low", "age": "65+"}
        )
        self.assertEqual(new_participant["protocol_version"], "v2.0")
        self.assertEqual(new_participant["version_no"], 2)
        self.assertEqual(new_participant["strata_factors"], ["risk", "age"])
        self.assertEqual(new_participant["block_size"], 6)

        participants = self.store.list_participants("coord", self.trial["id"])
        self.assertEqual(
            [p["protocol_version"] for p in participants],
            ["v1.0", "v1.0", "v2.0"],
        )
        self.assertEqual([p["id"] for p in participants[:2]], [1, 2])

        summary = self.store.trial_summary("coord", self.trial["id"])
        self.assertEqual(summary["trial"]["protocol_version"], "v2.0")
        self.assertEqual(
            {x["protocol_version"]: x["count"] for x in summary["by_protocol_version"]},
            {"v1.0": 2, "v2.0": 1},
        )
        self.assertEqual(
            [(p["external_id"], p["protocol_version"]) for p in summary["participants"]],
            [("S001-001", "v1.0"), ("S001-002", "v1.0"), ("S001-003", "v2.0")],
        )

        with self.store.connect() as conn:
            preserved_arms = [
                row["arm"]
                for row in conn.execute(
                    """SELECT a.arm FROM participants p
                       JOIN allocations a ON a.id=p.allocation_id
                       WHERE p.trial_id=? ORDER BY p.id LIMIT 2""",
                    (self.trial["id"],),
                ).fetchall()
            ]
            new_allocations = conn.execute(
                """SELECT COUNT(*) FROM allocations a
                   JOIN protocol_versions pv ON pv.id=a.protocol_version_id
                   WHERE pv.trial_id=? AND pv.protocol_version='v2.0'""",
                (self.trial["id"],),
            ).fetchone()[0]
            v1_unused_after = conn.execute(
                """SELECT COUNT(*) FROM allocations a
                   JOIN protocol_versions pv ON pv.id=a.protocol_version_id
                   WHERE pv.trial_id=? AND pv.protocol_version='v1.0' AND a.used_by IS NULL""",
                (self.trial["id"],),
            ).fetchone()[0]
        self.assertEqual(preserved_arms, old_arms)
        self.assertEqual(v1_unused_after, old_remaining)
        self.assertEqual(new_allocations, 6)

    def test_pending_unblinding_blocks_amendment_approval(self):
        participant = self.store.enroll(
            "site1", self.trial["id"], "S001-001", {"risk": "low"}
        )
        amendment = self.store.submit_protocol_amendment(
            "coord", self.trial["id"], "v2.0", ["A", "B"], ["risk"], 4, "seed-2026-009"
        )
        request = self.store.request_unblinding(
            "site1", participant["id"], "受试者发生疑似非预期严重不良反应"
        )
        with self.assertRaises(BusinessError) as ctx:
            self.store.approve_protocol_amendment("monitor1", self.trial["id"], amendment["id"])
        self.assertEqual(ctx.exception.code, "pending_unblinding")

        summary = self.store.trial_summary("coord", self.trial["id"])
        self.assertEqual(summary["trial"]["protocol_version"], "v1.0")
        self.assertEqual(summary["participants"][0]["protocol_version"], "v1.0")

        self.store.approve_unblinding("monitor1", request["id"])
        with self.assertRaises(BusinessError) as ctx:
            self.store.approve_protocol_amendment("monitor1", self.trial["id"], amendment["id"])
        self.assertEqual(ctx.exception.code, "pending_unblinding")
        self.store.approve_unblinding("monitor2", request["id"])
        approved = self.store.approve_protocol_amendment(
            "monitor1", self.trial["id"], amendment["id"]
        )
        self.assertEqual(approved["protocol_version"], "v2.0")
class LegacySchemaMigrationTests(unittest.TestCase):
    def test_old_database_is_backfilled_without_losing_allocation_rows(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "legacy.db"
            with sqlite3.connect(db_path) as conn:
                conn.executescript(
                    """
                    CREATE TABLE users(
                        id TEXT PRIMARY KEY, name TEXT NOT NULL, role TEXT NOT NULL,
                        site_id TEXT NOT NULL, active INTEGER NOT NULL DEFAULT 1
                    );
                    CREATE TABLE trials(
                        id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL UNIQUE,
                        protocol_version TEXT NOT NULL, status TEXT NOT NULL,
                        arms_json TEXT NOT NULL, strata_factors_json TEXT NOT NULL,
                        block_size INTEGER NOT NULL, seed TEXT NOT NULL,
                        created_by TEXT NOT NULL, created_at TEXT NOT NULL, started_at TEXT
                    );
                    CREATE TABLE strata(
                        id INTEGER PRIMARY KEY AUTOINCREMENT, trial_id INTEGER NOT NULL,
                        stratum_key TEXT NOT NULL, factors_json TEXT NOT NULL,
                        created_at TEXT NOT NULL, UNIQUE(trial_id,stratum_key)
                    );
                    CREATE TABLE allocations(
                        id INTEGER PRIMARY KEY AUTOINCREMENT, trial_id INTEGER NOT NULL,
                        stratum_id INTEGER NOT NULL, sequence INTEGER NOT NULL,
                        block_no INTEGER NOT NULL, arm TEXT NOT NULL,
                        used_by INTEGER, used_at TEXT, UNIQUE(stratum_id,sequence)
                    );
                    CREATE TABLE participants(
                        id INTEGER PRIMARY KEY AUTOINCREMENT, trial_id INTEGER NOT NULL,
                        site_id TEXT NOT NULL, external_id TEXT NOT NULL,
                        stratum_id INTEGER NOT NULL, allocation_id INTEGER NOT NULL UNIQUE,
                        allocation_code TEXT NOT NULL UNIQUE, status TEXT NOT NULL,
                        enrolled_by TEXT NOT NULL, created_at TEXT NOT NULL,
                        UNIQUE(trial_id,external_id)
                    );
                    CREATE TABLE unblinding_requests(
                        id INTEGER PRIMARY KEY AUTOINCREMENT, participant_id INTEGER NOT NULL,
                        requester_id TEXT NOT NULL, reason TEXT NOT NULL, status TEXT NOT NULL,
                        first_approver TEXT, second_approver TEXT, decided_at TEXT,
                        created_at TEXT NOT NULL
                    );
                    CREATE TABLE audit_log(
                        id INTEGER PRIMARY KEY AUTOINCREMENT, trial_id INTEGER,
                        actor_id TEXT NOT NULL, action TEXT NOT NULL,
                        detail TEXT NOT NULL, created_at TEXT NOT NULL
                    );
                    INSERT INTO users VALUES('coord','协调员','coordinator','CENTER',1);
                    INSERT INTO users VALUES('site1','中心','site','S001',1);
                    INSERT INTO users VALUES('monitor1','监查员','monitor','CENTER',1);
                    INSERT INTO users VALUES('monitor2','监查员','monitor','CENTER',1);
                    INSERT INTO trials VALUES(1,'旧库研究','v1.0','running','["A","B"]','["risk"]',4,'legacy-seed','coord','2026-01-01T00:00:00+00:00','2026-01-02T00:00:00+00:00');
                    INSERT INTO strata VALUES(1,1,'S001|{"risk":"low"}','{"risk":"low","site_id":"S001"}','2026-01-02T00:00:00+00:00');
                    INSERT INTO allocations VALUES(1,1,1,1,1,'A',1,'2026-01-03T00:00:00+00:00');
                    INSERT INTO allocations VALUES(2,1,1,2,1,'B',NULL,NULL);
                    INSERT INTO participants VALUES(1,1,'S001','S001-001',1,1,'CODE001','enrolled','site1','2026-01-03T00:00:00+00:00');
                    """
                )

            store = RandomizationStore(db_path)
            store.init_schema()
            participant = store.get_participant("coord", 1)
            self.assertEqual(participant["protocol_version"], "v1.0")
            amendment = store.submit_protocol_amendment(
                "coord", 1, "v2.0", ["A", "B"], ["risk"], 4, "new-seed-001"
            )
            store.approve_protocol_amendment("monitor1", 1, amendment["id"])
            new_participant = store.enroll("site1", 1, "S001-002", {"risk": "low"})
            self.assertEqual(new_participant["protocol_version"], "v2.0")
            with store.connect() as conn:
                self.assertEqual(conn.execute("SELECT arm FROM allocations WHERE id=1").fetchone()[0], "A")
                self.assertEqual(conn.execute("SELECT used_by FROM allocations WHERE id=2").fetchone()[0], None)
                self.assertEqual(conn.execute("SELECT protocol_version_id FROM participants WHERE id=1").fetchone()[0], 1)
                self.assertEqual(conn.execute("SELECT COUNT(*) FROM allocations WHERE protocol_version_id=2").fetchone()[0], 4)


if __name__ == "__main__":
    unittest.main()
