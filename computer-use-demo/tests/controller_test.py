from types import SimpleNamespace
from unittest import mock

import pytest
from docker.errors import APIError, NotFound

from computer_use_demo import controller


@pytest.mark.asyncio
async def test_create_runtime_recovers_from_same_session_name_conflict(monkeypatch):
    existing = SimpleNamespace(id="existing-runtime")
    containers = mock.Mock()
    containers.get.side_effect = [NotFound("missing"), NotFound("not visible yet"), existing]
    containers.run.side_effect = APIError("name already in use", response=mock.Mock(status_code=409))
    monkeypatch.setattr(controller, "client", SimpleNamespace(containers=containers))
    wait_for_desktop = mock.AsyncMock()
    monkeypatch.setattr(controller, "_wait_for_desktop", wait_for_desktop)

    result = await controller.create_runtime(
        controller.RuntimeCreate(session_id="sess_" + "a" * 32, runtime_token="x" * 32),
        x_internal_token=controller.settings.internal_token,
    )

    assert result == {
        "runtime_id": "existing-runtime",
        "runtime_host": "capsule-sess_" + "a" * 32,
        "created": False,
    }
    assert containers.get.call_count == 3
    wait_for_desktop.assert_awaited_once_with("capsule-sess_" + "a" * 32)


@pytest.mark.asyncio
async def test_create_runtime_replaces_a_stopped_capsule(monkeypatch):
    stopped = mock.Mock(id="stopped-runtime", status="exited")
    replacement = SimpleNamespace(id="replacement-runtime")
    containers = mock.Mock()
    containers.get.return_value = stopped
    containers.run.return_value = replacement
    monkeypatch.setattr(controller, "client", SimpleNamespace(containers=containers))
    wait_for_desktop = mock.AsyncMock()
    monkeypatch.setattr(controller, "_wait_for_desktop", wait_for_desktop)

    result = await controller.create_runtime(
        controller.RuntimeCreate(session_id="sess_" + "b" * 32, runtime_token="x" * 32),
        x_internal_token=controller.settings.internal_token,
    )

    assert result == {
        "runtime_id": "replacement-runtime",
        "runtime_host": "capsule-sess_" + "b" * 32,
        "created": True,
    }
    stopped.remove.assert_called_once_with(force=True, v=True)
    wait_for_desktop.assert_awaited_once_with("capsule-sess_" + "b" * 32)


@pytest.mark.asyncio
async def test_wait_for_desktop_retries_until_novnc_accepts_connections(monkeypatch):
    writer = mock.Mock()
    writer.wait_closed = mock.AsyncMock()
    open_connection = mock.AsyncMock(
        side_effect=[ConnectionRefusedError(), (mock.Mock(), writer)]
    )
    monkeypatch.setattr(controller.asyncio, "open_connection", open_connection)
    monkeypatch.setattr(controller.asyncio, "sleep", mock.AsyncMock())

    await controller._wait_for_desktop("capsule-test", attempts=2)

    assert open_connection.await_count == 2
    writer.close.assert_called_once_with()
    writer.wait_closed.assert_awaited_once_with()
