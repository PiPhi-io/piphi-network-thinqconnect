from __future__ import annotations

import json
from pathlib import Path

from piphi_network_thinqconnect.lib.normalization import DEVICE_TYPE_CATALOG, stable_client_id
from piphi_network_thinqconnect.contract.command import router as command_module


ROOT = Path(__file__).resolve().parents[1]


def _load_json(relative_path: str) -> dict:
    return json.loads((ROOT / relative_path).read_text(encoding="utf-8"))


def test_stable_client_id_is_account_scoped() -> None:
    account_client_id = stable_client_id(
        access_token="token-1",
        country_code="us",
        device_id="device-a",
    )

    assert account_client_id == stable_client_id(
        access_token="token-1",
        country_code="US",
        device_id="device-b",
    )
    assert account_client_id != stable_client_id(
        access_token="token-2",
        country_code="US",
        device_id="device-a",
    )
    assert account_client_id != stable_client_id(
        access_token="token-1",
        country_code="CA",
        device_id="device-a",
    )


def test_documented_commands_exist_in_manifest_and_behaviors() -> None:
    manifest = _load_json("src/manifest.json")
    behaviors = _load_json("src/behaviors.json")

    manifest_commands = set(manifest.get("commands", {}).keys())
    behavior_commands = {
        option["id"]
        for device in behaviors.get("devices", [])
        for group_name in ("actions", "stop")
        for option in device.get(group_name, [])
    }
    documented_commands = {
        command_id
        for device in DEVICE_TYPE_CATALOG.values()
        for command_id in device.get("documented_commands", [])
    }

    assert documented_commands
    assert documented_commands.issubset(manifest_commands)
    assert documented_commands.issubset(behavior_commands)


def test_automation_registry_matches_declared_behavior_commands() -> None:
    behaviors = _load_json("src/behaviors.json")
    declared_commands = {
        option["runtime"]["command"]
        for device in behaviors.get("devices", [])
        for option in device.get("actions", [])
    }
    registered_commands = {
        definition.command
        for definition in command_module.automation_registry.action_definitions
    }

    assert registered_commands == declared_commands


def test_behavior_capabilities_exist_in_manifest() -> None:
    manifest = _load_json("src/manifest.json")
    behaviors = _load_json("src/behaviors.json")

    manifest_capabilities = set(manifest.get("capabilities", {}).keys())
    behavior_capabilities = {
        capability
        for device in behaviors.get("devices", [])
        for capability in device.get("capabilities", [])
    }

    assert behavior_capabilities
    assert behavior_capabilities.issubset(manifest_capabilities)
