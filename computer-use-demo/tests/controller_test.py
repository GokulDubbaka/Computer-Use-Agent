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


@pytest.mark.asyncio
async def test_create_runtime_replaces_a_stopped_capsule(monkeypatch):
    stopped = mock.Mock(id="stopped-runtime", status="exited")
    replacement = SimpleNamespace(id="replacement-runtime")
    containers = mock.Mock()
    containers.get.return_value = stopped
    containers.run.return_value = replacement
    monkeypatch.setattr(controller, "client", SimpleNamespace(containers=containers))

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
