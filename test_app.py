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

    def test_protocol_amendment_flow(self):
        p1 = self.store.enroll("site1", self.trial["id"], "S001-001", {"risk": "low"})
        p2 = self.store.enroll("site1", self.trial["id"], "S001-002", {"risk": "low"})
        with self.assertRaises(BusinessError) as ctx:
            self.store.submit_amendment("site1", self.trial["id"], "v2.0", ["A", "B", "C"], ["risk", "age"], 6, "seed-2026-002")
        self.assertEqual(ctx.exception.code, "forbidden")
        amend = self.store.submit_amendment("coord", self.trial["id"], "v2.0", ["A", "B", "C"], ["risk", "age"], 6, "seed-2026-002")
        self.assertEqual(amend["status"], "pending")
        with self.assertRaises(BusinessError) as ctx:
            self.store.submit_amendment("coord", self.trial["id"], "v2.1", ["A", "B"], ["risk"], 4, "seed-2026-003")
        self.assertEqual(ctx.exception.code, "amendment_exists")
        summary = self.store.trial_summary("coord", self.trial["id"])
        self.assertEqual(summary["trial"]["protocol_version"], "v1.0")
        request = self.store.request_unblinding("site1", p1["id"], "受试者发生严重不良事件需要紧急处理")
        with self.assertRaises(BusinessError) as ctx:
            self.store.approve_amendment("monitor1", amend["id"])
        self.assertEqual(ctx.exception.code, "pending_unblinding")
        with self.assertRaises(BusinessError) as ctx:
            self.store.approve_amendment("coord", amend["id"])
        self.assertEqual(ctx.exception.code, "forbidden")
        self.store.approve_unblinding("monitor1", request["id"])
        unblinded = self.store.approve_unblinding("monitor2", request["id"])
        done = self.store.approve_amendment("monitor1", amend["id"])
        self.assertEqual(done["status"], "approved")
        self.assertEqual(self.store.get_participant("site1", p1["id"])["arm"], unblinded["arm"])
        p3 = self.store.enroll("site1", self.trial["id"], "S001-003", {"risk": "low", "age": "old"})
        self.assertEqual(p3["protocol_version"], "v2.0")
        participants = self.store.list_participants("coord", self.trial["id"])
        versions = {p["external_id"]: p["protocol_version"] for p in participants}
        self.assertEqual(versions, {"S001-001": "v1.0", "S001-002": "v1.0", "S001-003": "v2.0"})
        with self.store.connect() as conn:
            old_stratum = conn.execute(
                "SELECT stratum_id FROM participants WHERE id=?", (p1["id"],)
            ).fetchone()["stratum_id"]
            new_stratum = conn.execute(
                "SELECT stratum_id FROM participants WHERE id=?", (p3["id"],)
            ).fetchone()["stratum_id"]
            self.assertNotEqual(old_stratum, new_stratum)
            used_old = conn.execute(
                "SELECT COUNT(*) FROM allocations WHERE stratum_id=? AND used_by IS NOT NULL", (old_stratum,)
            ).fetchone()[0]
            self.assertEqual(used_old, 2)
            new_arms = [r["arm"] for r in conn.execute(
                "SELECT arm FROM allocations WHERE stratum_id=?", (new_stratum,)
            ).fetchall()]
            self.assertEqual(Counter(new_arms), Counter({"A": 6, "B": 6, "C": 6}))
            self.assertEqual(
                conn.execute("SELECT MIN(sequence) FROM allocations WHERE stratum_id=?", (new_stratum,)).fetchone()[0], 1
            )
        summary = self.store.trial_summary("coord", self.trial["id"])
        self.assertEqual(summary["trial"]["protocol_version"], "v2.0")
        self.assertEqual({v["protocol_version"]: v["count"] for v in summary["by_version"]}, {"v1.0": 2, "v2.0": 1})
        self.assertEqual({p["id"]: p["protocol_version"] for p in summary["participant_versions"]}[p3["id"]], "v2.0")
        amendments = self.store.list_amendments("monitor1", self.trial["id"])
        self.assertEqual(amendments[0]["status"], "approved")
        self.assertEqual(amendments[0]["confirmed_by"], "monitor1")


if __name__ == "__main__":
    unittest.main()
