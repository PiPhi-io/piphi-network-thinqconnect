from __future__ import annotations

import importlib
import time

from fastapi.testclient import TestClient
from piphi_runtime_testkit_python import (
    assert_event_sent,
    assert_telemetry_sent,
    build_runtime_headers,
)

from piphi_network_thinqconnect.app import app
from piphi_network_thinqconnect.lib.store import registry, runtime_context


config_module = importlib.import_module("piphi_network_thinqconnect.contract.config.routes")
command_module = importlib.import_module("piphi_network_thinqconnect.contract.command.router")
discovery_module = importlib.import_module("piphi_network_thinqconnect.contract.discovery.discovery")


class _DummyTask:
    def done(self) -> bool:
        return True

    def cancel(self) -> None:
        return None


def reset_runtime_state() -> None:
    registry.entries.clear()
    registry.state_snapshots.clear()
    registry.recent_events.clear()
    runtime_context.auth.container_id = ""
    runtime_context.auth.internal_token = ""
    runtime_context.process_state.background_tasks.clear()


def wait_for(condition, *, timeout: float = 2.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return
        time.sleep(0.05)
    raise AssertionError("Timed out waiting for background delivery to complete.")


def _ac_snapshot() -> dict:
    return {
        "device": {
            "deviceId": "device-ac-1",
            "deviceType": "DEVICE_AIR_CONDITIONER",
            "deviceName": "Living Room AC",
            "modelName": "DualCool",
        },
        "status": {
            "current_temperature_c": 23.5,
            "target_temperature_c": 24.0,
            "humidity": 45,
            "operation": "OFF",
            "mode": "COOL",
            "online": True,
        },
        "profile": {
            "main": {
                "resources": ["temperature", "humidity", "operation"],
            }
        },
        "available_controls": [
            {
                "method": "set_operation",
                "parameters": [
                    {"name": "operation", "options": ["ON", "OFF"]},
                ],
            },
            {
                "method": "set_mode",
                "parameters": [
                    {"name": "mode", "options": ["COOL", "HEAT", "DRY"]},
                ],
            },
            {
                "method": "set_target_temperature",
                "parameters": [
                    {
                        "name": "temperature",
                        "type": "number",
                        "min": 16,
                        "max": 30,
                        "unit": "°C",
                    }
                ],
            },
        ],
        "client_id": "generated-client-id",
    }


def _fridge_snapshot() -> dict:
    return {
        "device": {
            "deviceId": "fridge-1",
            "deviceType": "DEVICE_REFRIGERATOR",
            "deviceName": "Kitchen Fridge",
            "modelName": "LFXS26973S",
        },
        "status": {
            "door_state": "CLOSED",
            "current_state": "IDLE",
            "fresh_air_filter_remain_percent": 81,
            "target_temperature_c": 3,
            "online": True,
        },
        "profile": {},
        "available_controls": [
            {
                "method": "set_target_temperature",
                "parameters": [
                    {
                        "name": "temperature",
                        "type": "number",
                        "min": 1,
                        "max": 7,
                        "unit": "°C",
                    }
                ],
            },
            {
                "method": "set_express_mode",
                "parameters": [
                    {"name": "express_mode", "options": ["ON", "OFF"]},
                ],
            },
            {
                "method": "set_power_save",
                "parameters": [
                    {"name": "power_save", "options": ["ON", "OFF"]},
                ],
            },
        ],
        "client_id": "generated-client-id-fridge",
    }


def test_discovery_returns_mixed_thinq_devices(monkeypatch) -> None:
    reset_runtime_state()

    async def fake_discover_devices(**_kwargs):
        return [
            {
                "id": "device-ac-1",
                "device_id": "device-ac-1",
                "device_name": "Living Room AC",
                "device_type": "DEVICE_AIR_CONDITIONER",
            },
            {
                "id": "fridge-1",
                "device_id": "fridge-1",
                "device_name": "Kitchen Fridge",
                "device_type": "DEVICE_REFRIGERATOR",
            },
        ]

    monkeypatch.setattr(discovery_module.thinq_client, "discover_devices", fake_discover_devices)

    with TestClient(app) as client:
        response = client.post(
            "/discover",
            json={
                "access_token": "token-1",
                "country_code": "US",
                "client_id": "client-1",
            },
        )

    assert response.status_code == 200
    payload = response.json()
    assert len(payload["devices"]) == 2
    assert payload["devices"][0]["device_type"] == "DEVICE_AIR_CONDITIONER"
    assert payload["devices"][1]["device_type"] == "DEVICE_REFRIGERATOR"


def test_config_apply_sends_telemetry_and_event(mock_core, monkeypatch) -> None:
    reset_runtime_state()

    async def fake_fetch_device_snapshot(**_kwargs):
        return _ac_snapshot()

    monkeypatch.setattr(config_module.telemetry_client, "core_base_url", mock_core.base_url)
    monkeypatch.setattr(config_module.event_client, "core_base_url", mock_core.base_url)
    monkeypatch.setattr(config_module.thinq_client, "fetch_device_snapshot", fake_fetch_device_snapshot)
    monkeypatch.setattr(config_module, "start_device_poll_task", lambda **_kwargs: _DummyTask())

    headers = build_runtime_headers(container_id="runtime-456", internal_token="secret-token")

    with TestClient(app) as client:
        response = client.post(
            "/config",
            json={
                "id": "cfg-ac-1",
                "config_id": "cfg-ac-1",
                "device_id": "device-ac-1",
                "device_name": "Living Room AC",
                "device_type": "DEVICE_AIR_CONDITIONER",
                "access_token": "pat-1",
                "country_code": "US",
                "alias": "Main AC",
            },
            headers=headers,
        )
        assert response.status_code == 200
        assert response.json()["config_id"] == "cfg-ac-1"

        wait_for(lambda: len(mock_core.telemetry_requests) >= 1)
        wait_for(lambda: len(mock_core.event_requests) >= 1)

    telemetry_request = assert_telemetry_sent(mock_core, device_id="device-ac-1")
    event_request = assert_event_sent(
        mock_core,
        device_id="device-ac-1",
        config_id="cfg-ac-1",
        event_type="device.configured",
    )
    telemetry_headers = {key.lower(): value for key, value in telemetry_request.headers.items()}
    event_headers = {key.lower(): value for key, value in event_request.headers.items()}

    assert telemetry_headers["x-container-id"] == "runtime-456"
    assert telemetry_headers["x-piphi-integration-token"] == "secret-token"
    assert event_headers["x-container-id"] == "runtime-456"
    assert event_headers["x-piphi-integration-token"] == "secret-token"
    assert telemetry_request.json_body["metrics"]["current_temperature_c"] == 23.5
    assert telemetry_request.json_body["units"]["current_temperature_c"] == "°C"


def test_entities_and_state_include_read_only_device(mock_core, monkeypatch) -> None:
    reset_runtime_state()

    async def fake_fetch_device_snapshot(**_kwargs):
        return _fridge_snapshot()

    monkeypatch.setattr(config_module.telemetry_client, "core_base_url", mock_core.base_url)
    monkeypatch.setattr(config_module.event_client, "core_base_url", mock_core.base_url)
    monkeypatch.setattr(config_module.thinq_client, "fetch_device_snapshot", fake_fetch_device_snapshot)
    monkeypatch.setattr(config_module, "start_device_poll_task", lambda **_kwargs: _DummyTask())

    headers = build_runtime_headers(container_id="runtime-789", internal_token="secret-token")

    with TestClient(app) as client:
        config_response = client.post(
            "/config",
            json={
                "id": "cfg-fridge-1",
                "config_id": "cfg-fridge-1",
                "device_id": "fridge-1",
                "device_name": "Kitchen Fridge",
                "device_type": "DEVICE_REFRIGERATOR",
                "access_token": "pat-1",
                "country_code": "US",
            },
            headers=headers,
        )
        assert config_response.status_code == 200

        entities_response = client.get("/entities")
        assert entities_response.status_code == 200
        entities = entities_response.json()["entities"]
        assert len(entities) == 1
        assert entities[0]["device_type"] == "DEVICE_REFRIGERATOR"
        assert entities[0]["capabilities"] == [
            "door",
            "filter_life",
            "mode",
            "power",
            "refresh",
            "switch",
            "target_temperature",
            "temperature",
        ]
        assert [command["id"] for command in entities[0]["available_commands"]] == [
            "refresh",
            "set_target_temperature",
            "enable_express_mode",
            "disable_express_mode",
            "enable_power_save",
            "disable_power_save",
        ]
        temperature_command = next(
            command for command in entities[0]["available_commands"] if command["id"] == "set_target_temperature"
        )
        assert temperature_command["args_schema"]["temperature"]["minimum"] == 1.0
        assert temperature_command["args_schema"]["temperature"]["maximum"] == 7.0
        assert temperature_command["args_schema"]["temperature"]["unit"] == "°C"
        assert entities[0]["metadata"]["documented_commands"] == [
            "refresh",
            "set_target_temperature",
            "enable_power_save",
            "disable_power_save",
            "enable_eco_friendly",
            "disable_eco_friendly",
            "enable_express_mode",
            "disable_express_mode",
            "enable_rapid_freeze",
            "disable_rapid_freeze",
            "enable_sabbath_mode",
            "disable_sabbath_mode",
        ]
        assert entities[0]["metadata"]["device_profile"]["documented_resources"] == [
            "power_save",
            "eco_friendly",
            "sabbath",
            "refrigeration",
            "water_filter_info",
            "door_status",
            "temperature",
        ]
        assert "door_state" in entities[0]["metadata"]["device_profile"]["documented_properties"]

        state_response = client.get("/state", params={"device_id": "fridge-1"})
        assert state_response.status_code == 200
        assert state_response.json()["state"]["device_name"] == "Kitchen Fridge"
        assert state_response.json()["state"]["metrics"]["fresh_air_filter_remain_percent"] == 81


def test_command_route_rejects_unsupported_command(mock_core, monkeypatch) -> None:
    reset_runtime_state()

    async def fake_fetch_device_snapshot(**_kwargs):
        return _fridge_snapshot()

    monkeypatch.setattr(config_module.telemetry_client, "core_base_url", mock_core.base_url)
    monkeypatch.setattr(config_module.event_client, "core_base_url", mock_core.base_url)
    monkeypatch.setattr(config_module.thinq_client, "fetch_device_snapshot", fake_fetch_device_snapshot)
    monkeypatch.setattr(config_module, "start_device_poll_task", lambda **_kwargs: _DummyTask())

    headers = build_runtime_headers(container_id="runtime-999", internal_token="secret-token")

    with TestClient(app) as client:
        client.post(
            "/config",
            json={
                "id": "cfg-fridge-1",
                "config_id": "cfg-fridge-1",
                "device_id": "fridge-1",
                "device_name": "Kitchen Fridge",
                "device_type": "DEVICE_REFRIGERATOR",
                "access_token": "pat-1",
                "country_code": "US",
            },
            headers=headers,
        )
        response = client.post(
            "/command",
            json={
                "device_id": "fridge-1",
                "command": "turn_on",
                "args": {},
            },
        )

    assert response.status_code == 400
    assert "not supported" in response.json()["detail"]


def test_command_route_dispatches_family_specific_refrigerator_control(mock_core, monkeypatch) -> None:
    reset_runtime_state()
    executed: list[dict] = []

    async def fake_fetch_device_snapshot(**_kwargs):
        return _fridge_snapshot()

    async def fake_execute_device_command(**kwargs):
        executed.append(kwargs)
        return {"status": "ok", "control_method": kwargs["control_method"]}

    monkeypatch.setattr(config_module.telemetry_client, "core_base_url", mock_core.base_url)
    monkeypatch.setattr(config_module.event_client, "core_base_url", mock_core.base_url)
    monkeypatch.setattr(config_module.thinq_client, "fetch_device_snapshot", fake_fetch_device_snapshot)
    monkeypatch.setattr(config_module.thinq_client, "execute_device_command", fake_execute_device_command)
    monkeypatch.setattr(config_module, "start_device_poll_task", lambda **_kwargs: _DummyTask())

    headers = build_runtime_headers(container_id="runtime-fridge", internal_token="secret-token")

    with TestClient(app) as client:
        config_response = client.post(
            "/config",
            json={
                "id": "cfg-fridge-1",
                "config_id": "cfg-fridge-1",
                "device_id": "fridge-1",
                "device_name": "Kitchen Fridge",
                "device_type": "DEVICE_REFRIGERATOR",
                "access_token": "pat-1",
                "country_code": "US",
            },
            headers=headers,
        )
        assert config_response.status_code == 200

        headers["X-PiPhi-Idempotency-Key"] = "thinq-action-idempotency-1"
        response = client.post(
            "/command",
            json={
                "device_id": "fridge-1",
                "command": "enable_express_mode",
                "args": {},
            },
            headers=headers,
        )
        replay = client.post(
            "/command",
            json={
                "device_id": "fridge-1",
                "command": "enable_express_mode",
                "args": {},
            },
            headers=headers,
        )

    assert response.status_code == 200
    assert replay.status_code == 200
    assert response.json()["replayed"] is False
    assert replay.json()["replayed"] is True
    assert len(executed) == 1
    assert executed[0]["control_method"] == "set_express_mode"
    assert executed[0]["control_params"] == {"express_mode": "ON"}
    assert response.json()["result"]["status"] == "ok"


def test_command_route_dispatches_safe_profile_mapped_control(mock_core, monkeypatch) -> None:
    reset_runtime_state()
    executed: list[dict] = []

    async def fake_fetch_device_snapshot(**_kwargs):
        return _ac_snapshot()

    async def fake_execute_device_command(**kwargs):
        executed.append(kwargs)
        return {"status": "ok", "control_method": kwargs["control_method"]}

    monkeypatch.setattr(config_module.telemetry_client, "core_base_url", mock_core.base_url)
    monkeypatch.setattr(config_module.event_client, "core_base_url", mock_core.base_url)
    monkeypatch.setattr(config_module.thinq_client, "fetch_device_snapshot", fake_fetch_device_snapshot)
    monkeypatch.setattr(config_module.thinq_client, "execute_device_command", fake_execute_device_command)
    monkeypatch.setattr(config_module, "start_device_poll_task", lambda **_kwargs: _DummyTask())

    headers = build_runtime_headers(container_id="runtime-321", internal_token="secret-token")

    with TestClient(app) as client:
        config_response = client.post(
            "/config",
            json={
                "id": "cfg-ac-1",
                "config_id": "cfg-ac-1",
                "device_id": "device-ac-1",
                "device_name": "Living Room AC",
                "device_type": "DEVICE_AIR_CONDITIONER",
                "access_token": "pat-1",
                "country_code": "US",
            },
            headers=headers,
        )
        assert config_response.status_code == 200

        response = client.post(
            "/command",
            json={
                "device_id": "device-ac-1",
                "command": "set_mode",
                "args": {"mode": "HEAT"},
            },
        )

    assert response.status_code == 200
    assert executed[0]["control_method"] == "set_mode"
    assert executed[0]["control_params"] == {"mode": "HEAT"}
    assert response.json()["result"]["status"] == "ok"


def test_entities_expose_documented_profile_for_device_type(mock_core, monkeypatch) -> None:
    reset_runtime_state()

    async def fake_fetch_device_snapshot(**_kwargs):
        return _ac_snapshot()

    monkeypatch.setattr(config_module.telemetry_client, "core_base_url", mock_core.base_url)
    monkeypatch.setattr(config_module.event_client, "core_base_url", mock_core.base_url)
    monkeypatch.setattr(config_module.thinq_client, "fetch_device_snapshot", fake_fetch_device_snapshot)
    monkeypatch.setattr(config_module, "start_device_poll_task", lambda **_kwargs: _DummyTask())

    headers = build_runtime_headers(container_id="runtime-222", internal_token="secret-token")

    with TestClient(app) as client:
        config_response = client.post(
            "/config",
            json={
                "id": "cfg-ac-1",
                "config_id": "cfg-ac-1",
                "device_id": "device-ac-1",
                "device_name": "Living Room AC",
                "device_type": "DEVICE_AIR_CONDITIONER",
                "access_token": "pat-1",
                "country_code": "US",
            },
            headers=headers,
        )
        assert config_response.status_code == 200

        entities_response = client.get("/entities")
        assert entities_response.status_code == 200
        entity = entities_response.json()["entities"][0]

    assert entity["entity_type"] == "thermostat"
    assert entity["capabilities"] == [
        "brightness",
        "fan_speed",
        "filter_life",
        "humidity",
        "mode",
        "pm1",
        "pm10",
        "pm2",
        "refresh",
        "switch",
        "target_temperature",
        "temperature",
    ]
    assert entity["metadata"]["documented_commands"] == [
        "refresh",
        "turn_on",
        "turn_off",
        "toggle",
        "set_mode",
        "set_target_temperature",
        "set_fan_speed",
        "set_brightness",
    ]
    assert entity["metadata"]["device_profile"]["documented_resources"] == [
        "operation",
        "temperature",
        "air_flow",
        "air_quality_sensor",
        "filter_info",
        "display",
        "wind_direction",
    ]
    assert "current_temperature_c" in entity["metadata"]["device_profile"]["documented_properties"]
    assert entity["available_commands"][0]["id"] == "refresh"
