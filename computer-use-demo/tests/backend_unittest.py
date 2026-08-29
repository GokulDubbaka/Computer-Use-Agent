import sqlite3
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

from computer_use_demo import server
from computer_use_demo.session_store import SessionStore, SessionStoreConfig


class SessionStoreTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.store = SessionStore(
            SessionStoreConfig(db_path=Path(self.temp_dir.name) / "sessions.sqlite3")
        )

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_run_creation_is_idempotent(self):
        session = self.store.create_session(title="test")
        first, first_created = self.store.create_run(
            session_id=session["id"], request_id="request-1"
        )
        second, second_created = self.store.create_run(
            session_id=session["id"], request_id="request-1"
        )
        self.assertTrue(first_created)
        self.assertFalse(second_created)
        self.assertEqual(first["id"], second["id"])

    def test_only_one_distinct_active_run_per_session(self):
        session = self.store.create_session(title="test")
        self.store.create_run(session_id=session["id"], request_id="request-1")
        with self.assertRaises(sqlite3.IntegrityError):
            self.store.create_run(session_id=session["id"], request_id="request-2")

    def test_concurrent_events_are_strictly_ordered(self):
        session = self.store.create_session(title="test")

        def append(index):
            return self.store.append_event(session["id"], "test.event", {"index": index})

        with ThreadPoolExecutor(max_workers=12) as executor:
            list(executor.map(append, range(50)))
        events = self.store.list_events(session["id"], limit=100)
        self.assertEqual([event["sequence"] for event in events], list(range(1, 51)))

    def test_messages_survive_store_restart(self):
        session = self.store.create_session(title="test")
        self.store.create_message(session["id"], role="user", content="hello")
        reopened = SessionStore(SessionStoreConfig(db_path=self.store.config.db_path))
        self.assertEqual(reopened.list_messages(session["id"])[0]["content"], "hello")

    def test_restart_recovery_releases_session(self):
        session = self.store.create_session(title="test")
        run, _ = self.store.create_run(session_id=session["id"], request_id="request-1")
        self.assertEqual(self.store.recover_incomplete_runs(), 1)
        self.assertEqual(self.store.get_run(run["id"])["status"], "failed")
        self.assertIsNone(self.store.get_session(session["id"])["active_run_id"])


class ApiTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.store = SessionStore(
            SessionStoreConfig(db_path=Path(self.temp_dir.name) / "api.sqlite3")
        )
        self.store_patch = patch.object(server, "store", self.store)
        self.store_patch.start()
        self.client = TestClient(server.app)
        self.client.__enter__()

    def tearDown(self):
        self.client.__exit__(None, None, None)
        self.store_patch.stop()
        self.temp_dir.cleanup()

    def test_session_message_idempotent_run_and_replay(self):
        self.assertEqual(self.client.get("/health").json(), {"status": "ok"})
        created = self.client.post("/api/sessions", json={"title": "demo"})
        self.assertEqual(created.status_code, 201)
        session_id = created.json()["id"]
        message = self.client.post(
            f"/api/sessions/{session_id}/messages",
            json={"role": "user", "content": "hello"},
        )
        self.assertEqual(message.status_code, 201)
        headers = {"Idempotency-Key": "same-request"}
        first = self.client.post(f"/api/sessions/{session_id}/runs", headers=headers, json={})
        second = self.client.post(f"/api/sessions/{session_id}/runs", headers=headers, json={})
        self.assertEqual(first.status_code, 201)
        self.assertEqual(second.status_code, 200)
        self.assertEqual(first.json()["id"], second.json()["id"])
        events = self.client.get(f"/api/sessions/{session_id}/events?after=1").json()["items"]
        self.assertTrue(events)
        self.assertTrue(all(event["sequence"] > 1 for event in events))

    def test_missing_session_returns_404(self):
        self.assertEqual(self.client.get("/api/sessions/missing").status_code, 404)

    def test_idle_session_can_be_deleted(self):
        created = self.client.post("/api/sessions", json={"title": "temporary"})
        session_id = created.json()["id"]
        self.assertEqual(self.client.delete(f"/api/sessions/{session_id}").status_code, 204)
        self.assertEqual(self.client.get(f"/api/sessions/{session_id}").status_code, 404)


if __name__ == "__main__":
    unittest.main(verbosity=2)
