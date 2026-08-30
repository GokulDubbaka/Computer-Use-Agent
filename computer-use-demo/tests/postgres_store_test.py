from contextlib import nullcontext
from unittest.mock import MagicMock

from computer_use_demo.postgres_store import PostgresStore, PostgresStoreConfig


def test_session_snapshot_selects_latest_events_in_chronological_order(monkeypatch):
    connection = MagicMock()
    session_cursor = MagicMock()
    session_cursor.fetchone.return_value = {"id": "sess_test", "status": "ready"}
    run_cursor = MagicMock()
    run_cursor.fetchone.return_value = None
    messages_cursor = MagicMock()
    messages_cursor.fetchall.return_value = []
    events_cursor = MagicMock()
    events_cursor.fetchall.return_value = [{"sequence": 4}, {"sequence": 5}]
    connection.execute.side_effect = [
        MagicMock(),
        session_cursor,
        run_cursor,
        messages_cursor,
        events_cursor,
    ]

    store = PostgresStore(PostgresStoreConfig("postgresql://unused"))
    monkeypatch.setattr(store, "initialize", lambda: None)
    monkeypatch.setattr(store, "_connect", lambda: nullcontext(connection))

    snapshot = store.get_session_snapshot("sess_test", event_limit=2)

    assert snapshot is not None
    assert [event["sequence"] for event in snapshot["events"]] == [4, 5]
    event_query, parameters = connection.execute.call_args_list[-1].args
    assert "ORDER BY sequence DESC LIMIT %s" in event_query
    assert event_query.strip().endswith("ORDER BY sequence")
    assert parameters == ("sess_test", 2)
