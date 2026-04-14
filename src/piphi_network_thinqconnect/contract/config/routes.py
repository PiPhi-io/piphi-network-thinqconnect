from __future__ import annotations

import asyncio
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from piphi_runtime_kit_python import (
    ConfigSyncCoordinator,
    EventClient,
    RuntimeConfigApplyResponse,
    RuntimeConfigRemoveResponse,
    TelemetryClient,
    build_config_apply_response,
    build_config_remove_response,
    create_tracked_task,
    format_config_apply_log,
    format_runtime_auth_sync_log,
    schedule_event_delivery,
    schedule_telemetry_delivery,
    shutdown_background_tasks as shutdown_runtime_background_tasks,
)
from piphi_runtime_kit_python.fastapi import (
    get_payload_container_id,
    sync_runtime_auth_from_fastapi_payload,
)

from piphi_network_thinqconnect.lib.client import ThinQApiClient, ThinQClientError
from piphi_network_thinqconnect.lib.logging import logger
from piphi_network_thinqconnect.lib.normalization import normalize_device_snapshot, stable_client_id
from piphi_network_thinqconnect.lib.schemas import (
    DeconfigureConfig,
    RuntimeConfigSnapshot,
    RuntimeConfigSyncResponse,
    ThinQDeviceConfig,
)
from piphi_network_thinqconnect.lib.store import (
    CORE_BASE_URL,
    INTEGRATION_ID,
    append_event,
    config_sync,
    event_client,
    get_runtime_context,
    registry,
    telemetry_client,
    update_device_state,
)


config_router = APIRouter(tags=["config"])
POLL_INTERVAL_SECONDS = 60
TELEMETRY_REQUEST_TIMEOUT_SECONDS = 3.0
EVENT_REQUEST_TIMEOUT_SECONDS = 3.0
runtime_context = get_runtime_context()
thinq_client = ThinQApiClient()
telemetry_client.core_base_url = CORE_BASE_URL
telemetry_client.timeout_seconds = TELEMETRY_REQUEST_TIMEOUT_SECONDS
event_client.core_base_url = CORE_BASE_URL
event_client.timeout_seconds = EVENT_REQUEST_TIMEOUT_SECONDS
config_sync = ConfigSyncCoordinator(process_state=runtime_context.process_state)


def _sync_runtime_auth_from_request(request: Request, payload: Any | None = None) -> None:
    parsed_headers = sync_runtime_auth_from_fastapi_payload(runtime_context, request, payload)
    logger.info(
        format_runtime_auth_sync_log(
            parsed_headers,
            payload_container_id=get_payload_container_id(payload),
        )
    )


def schedule_event_send(
    *,
    event_type: str,
    device: dict[str, Any],
    payload: dict[str, Any] | None = None,
    source: str = "lg_thinq_connect_runtime",
    severity: str = "info",
) -> None:
    schedule_event_delivery(
        process_state=runtime_context.process_state,
        event_client=event_client,
        auth_context=runtime_context.auth,
        event_type=event_type,
        device=device,
        payload=payload,
        source=source,
        severity=severity,
        record_event=append_event,
        on_skipped=lambda reason, details: logger.debug(
            "event_send_skipped reason=%s event_type=%s device_id=%s",
            reason,
            details.get("event_type") or event_type,
            details.get("device_id") or "unknown",
        ),
        on_error=lambda exc, details: logger.exception(
            "event_send_unexpected_error event_type=%s device_id=%s error=%s",
            details.get("event_type") or event_type,
            details.get("device_id") or "unknown",
            exc,
        ),
    )


async def shutdown_background_tasks() -> None:
    await shutdown_runtime_background_tasks(runtime_context.process_state)


def _poll_interval(entry: dict[str, Any]) -> int:
    value = entry.get("poll_interval_seconds")
    if isinstance(value, int):
        return max(15, value)
    return POLL_INTERVAL_SECONDS


def resolve_entry_id(device_or_config_id: str) -> str | None:
    if registry.get(device_or_config_id) is not None:
        return device_or_config_id
    for entry_id in registry.ids():
        entry = registry.get(entry_id) or {}
        if str(entry.get("device_id") or "") == str(device_or_config_id):
            return entry_id
    return None


def _build_telemetry_metrics(normalized_state: dict[str, Any]) -> dict[str, Any]:
    metrics = normalized_state.get("metrics")
    return metrics if isinstance(metrics, dict) else {}


def _build_telemetry_units(normalized_state: dict[str, Any]) -> dict[str, Any]:
    units = normalized_state.get("units")
    return units if isinstance(units, dict) else {}


async def fetch_and_store_state(*, device_id: str) -> dict[str, Any]:
    entry_id = resolve_entry_id(device_id)
    if entry_id is None:
        raise HTTPException(status_code=404, detail=f"Device '{device_id}' is not configured")

    device = registry.get(entry_id)
    if device is None:
        raise HTTPException(status_code=404, detail=f"Device '{device_id}' is not configured")

    try:
        snapshot = await thinq_client.fetch_device_snapshot(
            access_token=device["access_token"],
            country_code=device["country_code"],
            device_id=device["device_id"],
            device_type=device["device_type"],
            client_id=device.get("client_id"),
        )
    except ThinQClientError as exc:
        raise HTTPException(status_code=502, detail=f"ThinQ state fetch failed: {exc}") from exc

    normalized_state = normalize_device_snapshot(
        raw_device=snapshot["device"],
        raw_status=snapshot.get("status"),
        raw_profile=snapshot.get("profile"),
        raw_available_controls=snapshot.get("available_controls"),
    )
    normalized_state["client_id"] = snapshot.get("client_id") or device.get("client_id")
    device["client_id"] = normalized_state["client_id"]
    latest_state = update_device_state(device_id=entry_id, state=normalized_state)

    resolved_container_id = device.get("container_id")
    if resolved_container_id:
        schedule_telemetry_delivery(
            process_state=runtime_context.process_state,
            telemetry_client=telemetry_client,
            auth_context=runtime_context.auth,
            device_id=str(device.get("device_id") or entry_id),
            metrics=_build_telemetry_metrics(normalized_state),
            container_id=resolved_container_id,
            units=_build_telemetry_units(normalized_state),
            on_skipped=lambda reason, details: logger.warning(
                "telemetry_send_skipped reason=%s device_id=%s",
                reason,
                details.get("device_id") or str(device.get("device_id") or entry_id),
            ),
            on_error=lambda exc, details: logger.exception(
                "telemetry_send_unexpected_error device_id=%s error=%s",
                details.get("device_id") or str(device.get("device_id") or entry_id),
                exc,
            ),
        )
    return latest_state


async def poll_device_state(*, device_id: str) -> None:
    while True:
        entry_id = resolve_entry_id(device_id)
        if entry_id is None:
            return
        entry = registry.get(entry_id)
        if entry is None:
            return

        try:
            await fetch_and_store_state(device_id=entry_id)
            logger.info("state_poll_success device_id=%s", device_id)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("state_poll_error device_id=%s error=%s", device_id, exc)

        await asyncio.sleep(_poll_interval(entry))


def start_device_poll_task(*, device_id: str) -> asyncio.Task[Any]:
    return create_tracked_task(
        poll_device_state(device_id=device_id),
        process_state=runtime_context.process_state,
    )


async def trigger_refresh(device_id: str) -> dict[str, Any]:
    return await fetch_and_store_state(device_id=device_id)


async def run_command_for_device(
    *,
    device_id: str,
    command: str,
    args: dict[str, Any] | None = None,
) -> dict[str, Any]:
    entry_id = resolve_entry_id(device_id)
    if entry_id is None:
        raise HTTPException(status_code=404, detail=f"Device '{device_id}' is not configured")

    device = registry.get(entry_id)
    if device is None:
        raise HTTPException(status_code=404, detail=f"Device '{device_id}' is not configured")

    latest_state = device.get("latest_state") if isinstance(device.get("latest_state"), dict) else {}
    command_map = latest_state.get("command_map") if isinstance(latest_state.get("command_map"), dict) else {}
    mapping = command_map.get(command)
    if command == "toggle" and mapping is None:
        summary = latest_state.get("summary") if isinstance(latest_state.get("summary"), dict) else {}
        is_on = bool(summary.get("is_on"))
        mapping = command_map.get("turn_off") if is_on else command_map.get("turn_on")
    if mapping is None:
        raise HTTPException(
            status_code=400,
            detail=f"Command '{command}' is not supported for device '{device_id}'",
        )

    control_method = str(mapping.get("control_method") or "").strip()
    if not control_method:
        raise HTTPException(status_code=400, detail=f"Command '{command}' is missing a control method mapping")

    base_params = mapping.get("control_params") if isinstance(mapping.get("control_params"), dict) else {}
    arg_map = mapping.get("arg_map") if isinstance(mapping.get("arg_map"), dict) else {}
    effective_params = dict(base_params)
    args = args or {}

    for user_key, value in args.items():
        target_key = str(arg_map.get(user_key) or user_key)
        effective_params[target_key] = value

    try:
        result = await thinq_client.execute_device_command(
            access_token=device["access_token"],
            country_code=device["country_code"],
            device_id=device["device_id"],
            device_type=device["device_type"],
            control_method=control_method,
            control_params=effective_params,
            client_id=device.get("client_id"),
        )
    except ThinQClientError as exc:
        schedule_event_send(
            event_type="device.command.failed",
            device=device,
            payload={
                "command": command,
                "control_method": control_method,
                "args": args,
                "error": str(exc),
            },
            severity="error",
        )
        raise HTTPException(status_code=502, detail=f"ThinQ command failed: {exc}") from exc

    schedule_event_send(
        event_type="device.command.executed",
        device=device,
        payload={
            "command": command,
            "control_method": control_method,
            "args": args,
            "control_params": effective_params,
        },
    )
    return result


async def remove_device_config(device_id: str) -> bool:
    entry_id = resolve_entry_id(device_id)
    if entry_id is None:
        return False

    existing_entry = registry.get(entry_id)
    if existing_entry and existing_entry.get("task") and not existing_entry["task"].done():
        existing_entry["task"].cancel()
        try:
            await existing_entry["task"]
        except asyncio.CancelledError:
            pass

    removed = registry.get(entry_id) is not None
    if existing_entry is not None:
        schedule_event_send(
            event_type="device.deconfigured",
            device=existing_entry,
            payload={
                "device_name": existing_entry.get("device_name"),
                "device_type": existing_entry.get("device_type"),
            },
        )
    registry.remove(entry_id)
    return removed


async def apply_device_config(payload: ThinQDeviceConfig) -> dict[str, Any]:
    logger.info(format_config_apply_log(payload))
    resolved_container_id, _ = runtime_context.auth.resolve(container_id=payload.container_id)
    resolved_client_id = payload.client_id or stable_client_id(
        access_token=payload.access_token,
        country_code=payload.country_code,
        device_id=payload.device_id,
    )

    await remove_device_config(payload.id)
    task = start_device_poll_task(device_id=payload.id)
    registry.set(
        payload.id,
        {
            "task": task,
            "config_id": payload.config_id or payload.id,
            "container_id": resolved_container_id,
            "device_id": payload.device_id,
            "device_name": payload.device_name,
            "device_type": payload.device_type,
            "access_token": payload.access_token,
            "country_code": payload.country_code.upper(),
            "client_id": resolved_client_id,
            "alias": payload.alias,
            "integration_id": payload.integration_id or INTEGRATION_ID,
            "poll_interval_seconds": payload.poll_interval_seconds or POLL_INTERVAL_SECONDS,
        },
    )

    try:
        await trigger_refresh(payload.id)
    except HTTPException as exc:
        logger.warning("thinq_initial_refresh_failed device_id=%s detail=%s", payload.id, exc.detail)

    device_entry = registry.get(payload.id) or {}
    schedule_event_send(
        event_type="device.configured",
        device=device_entry,
        payload={
            "device_name": payload.device_name,
            "device_type": payload.device_type,
            "alias": payload.alias,
        },
    )

    return build_config_apply_response(
        config_id=payload.config_id or payload.id,
        container_id=resolved_container_id,
        metadata={
            "device_id": payload.device_id,
            "device_type": payload.device_type,
            "poll_interval_seconds": payload.poll_interval_seconds or POLL_INTERVAL_SECONDS,
        },
    ).model_dump()


async def apply_runtime_config_snapshot(payload: RuntimeConfigSnapshot) -> RuntimeConfigSyncResponse:
    async def apply_config_with_context(config: ThinQDeviceConfig) -> dict[str, Any]:
        effective_config = config.model_copy(
            update={
                "integration_id": config.integration_id or payload.integration_id,
                "container_id": config.container_id or payload.container_id,
            }
        )
        return await apply_device_config(effective_config)

    response = await config_sync.apply_snapshot(
        snapshot=payload,
        active_config_ids=registry.ids(),
        apply_config=apply_config_with_context,
        remove_config=remove_device_config,
        get_active_config_ids=registry.ids,
    )
    return RuntimeConfigSyncResponse(**response.model_dump())


@config_router.post("/config")
async def config(payload: ThinQDeviceConfig, request: Request) -> RuntimeConfigApplyResponse:
    _sync_runtime_auth_from_request(request, payload)
    return RuntimeConfigApplyResponse.model_validate(await apply_device_config(payload))


@config_router.post("/configs/sync", response_model=RuntimeConfigSyncResponse)
async def sync_configs(payload: RuntimeConfigSnapshot, request: Request) -> RuntimeConfigSyncResponse:
    _sync_runtime_auth_from_request(request, payload)
    return await apply_runtime_config_snapshot(payload)


@config_router.post("/config/sync", response_model=RuntimeConfigSyncResponse)
async def sync_config(payload: RuntimeConfigSnapshot, request: Request) -> RuntimeConfigSyncResponse:
    _sync_runtime_auth_from_request(request, payload)
    return await apply_runtime_config_snapshot(payload)


@config_router.post("/deconfigure")
async def deconfigure_device(payload: DeconfigureConfig) -> RuntimeConfigRemoveResponse:
    device_id = payload.config.get("id")
    if device_id is None:
        raise HTTPException(status_code=400, detail="Missing config.id")

    removed = await remove_device_config(str(device_id))
    return build_config_remove_response(
        config_id=str(device_id),
        removed=removed,
        metadata={"remaining_configs": registry.ids()},
    )
