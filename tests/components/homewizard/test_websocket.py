"""Tests for HomeWizard WebSocket coordinator updates."""

import asyncio
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import MagicMock

from homewizard_energy import WebSocketTopic
from homewizard_energy.models import CombinedModels
import pytest

from homeassistant.components.homewizard.coordinator import (
    HWEnergyDeviceUpdateCoordinator,
)
from homeassistant.core import HomeAssistant
from tests.common import MockConfigEntry


async def test_websocket_measurement_update(
    hass: HomeAssistant,
    mock_config_entry_v2: MockConfigEntry,
    mock_homewizardenergy_v2: MagicMock,
) -> None:
    """Test WebSocket measurements update the coordinator."""
    mock_config_entry_v2.add_to_hass(hass)

    api = mock_homewizardenergy_v2
    original: CombinedModels = api.combined.return_value

    coordinator = HWEnergyDeviceUpdateCoordinator(
        hass, mock_config_entry_v2, api
    )
    coordinator.async_set_updated_data(original)

    measurement = replace(
        original.measurement,
        power_w=1234.0,
    )

    websocket = MagicMock()
    websocket.__aenter__.return_value = websocket
    api.websocket.return_value = websocket

    received = asyncio.Event()

    async def events():
        yield SimpleNamespace(
            type=WebSocketTopic.MEASUREMENT,
            data=measurement,
        )
        received.set()
        await asyncio.Future()

    websocket.events_typed.return_value = events()

    task = asyncio.create_task(
        coordinator.async_process_websocket_events(api)
    )

    try:
        await asyncio.wait_for(received.wait(), timeout=5)

        assert coordinator.data.measurement.power_w == 1234.0
        assert coordinator.data.device == original.device
        assert coordinator.data.system == original.system
        assert coordinator.data.state == original.state
        assert coordinator.data.batteries == original.batteries
        assert coordinator._websocket_active

        websocket.subscribe.assert_awaited_once_with(
            WebSocketTopic.MEASUREMENT
        )
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    assert not coordinator._websocket_active


async def test_http_does_not_overwrite_websocket_measurements(
    hass: HomeAssistant,
    mock_config_entry_v2: MockConfigEntry,
    mock_homewizardenergy_v2: MagicMock,
) -> None:
    """Test HTTP refresh preserves active WebSocket measurements."""
    mock_config_entry_v2.add_to_hass(hass)

    api = mock_homewizardenergy_v2
    original = api.combined.return_value

    coordinator = HWEnergyDeviceUpdateCoordinator(
        hass, mock_config_entry_v2, api
    )

    websocket_measurement = replace(
        original.measurement,
        power_w=1500.0,
    )

    coordinator.data = replace(
        original,
        measurement=websocket_measurement,
    )
    coordinator._websocket_active = True

    api.combined.return_value = replace(
        original,
        measurement=replace(original.measurement, power_w=900.0),
    )

    result = await coordinator._async_update_data()

    assert result.measurement.power_w == 1500.0
    assert result.device == original.device


async def test_http_fallback_when_websocket_disconnected(
    hass: HomeAssistant,
    mock_config_entry_v2: MockConfigEntry,
    mock_homewizardenergy_v2: MagicMock,
) -> None:
    """Test HTTP measurements are used when WebSocket is unavailable."""
    mock_config_entry_v2.add_to_hass(hass)

    api = mock_homewizardenergy_v2
    original = api.combined.return_value

    coordinator = HWEnergyDeviceUpdateCoordinator(
        hass, mock_config_entry_v2, api
    )

    coordinator.data = replace(
        original,
        measurement=replace(original.measurement, power_w=1500.0),
    )
    coordinator._websocket_active = False

    api.combined.return_value = replace(
        original,
        measurement=replace(original.measurement, power_w=900.0),
    )

    result = await coordinator._async_update_data()

    assert result.measurement.power_w == 900.0
    assert result.device == original.device
