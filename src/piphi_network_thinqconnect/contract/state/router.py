from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from piphi_network_thinqconnect.contract.config.routes import resolve_entry_id, trigger_refresh
from piphi_network_thinqconnect.lib.store import get_primary_device, registry


router = APIRouter(tags=["state"])


@router.get("/state")
async def get_state(device_id: str | None = Query(default=None)) -> dict:
    if device_id is None:
        primary_device = get_primary_device()
        if primary_device is None:
            raise HTTPException(status_code=404, detail="No configured device found")

        if len(registry.ids()) > 1:
            return {
                "devices": list(registry.state_snapshots.values()),
                "count": len(registry.state_snapshots),
            }
        device_id = primary_device["device_id"]

    entry_id = resolve_entry_id(device_id)
    if entry_id is None:
        raise HTTPException(status_code=404, detail=f"No state available for device '{device_id}'")

    if entry_id not in registry.state_snapshots and registry.get(entry_id) is not None:
        await trigger_refresh(entry_id)

    latest_state = registry.state_snapshots.get(entry_id)
    if latest_state is None:
        raise HTTPException(status_code=404, detail=f"No state available for device '{device_id}'")
    return latest_state
