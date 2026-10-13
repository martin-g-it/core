"""Tests for HomeWizard WebSocket updates and HTTP fallback."""

import asyncio
from collections.abc import AsyncIterator
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from homewizard_energy import WebSocketTopic
from homewizard_energy.errors import RequestError
from homewizard_energy.models import CombinedModels
import pytest

from homeassistant.components.homewizard.const import UPDATE_INTERVAL
from homeassistant.components.homewizard.coordinator import (
    HWEnergyDeviceUpdateCoordinator,
)
from homeassistant.core import HomeAssistant

from tests.common import MockConfigEntry


def _mock_websocket(
    events: AsyncIterator[SimpleNamespace],
) -> tuple[MagicMock, MagicMock]:
    """Build a mock async WebSocket context and client."""
    websocket = MagicMock()
    websocket.subscribe = AsyncMock()
    websocket.events_typed.return_value = events

    context = MagicMock()
    context.__aenter__ = AsyncMock(return_value=websocket)
    context.__aexit__ = AsyncMock(return_value=None)
    return context, websocket


async def test_websocket_measurement_update(
    hass: HomeAssistant,
    mock_config_entry_v2: MockConfigEntry,
    mock_homewizardenergy_v2: MagicMock,
) -> None:
    """Test WSS updates P1 measurements and disables HTTP polling."""
    mock_config_entry_v2.add_to_hass(hass)
    api = mock_homewizardenergy_v2
    original: CombinedModels = api.combined.return_value
    coordinator = HWEnergyDeviceUpdateCoordinator(hass, mock_config_entry_v2, api)
    coordinator.async_set_updated_data(original)
    api.combined.reset_mock()

    measurement = replace(original.measurement, power_w=1234.0)
    received = asyncio.Event()

    async def events() -> AsyncIterator[SimpleNamespace]:
        yield SimpleNamespace(type=WebSocketTopic.MEASUREMENT, data=measurement)
        received.set()
        await asyncio.Future()

    context, websocket = _mock_websocket(events())
    api.websocket.return_value = context

    task = asyncio.create_task(coordinator.async_process_websocket_events(api))
    try:
        await asyncio.wait_for(received.wait(), timeout=5)
        assert coordinator._websocket_active
        assert coordinator.update_interval is None
        assert coordinator.data.measurement.power_w == 1234.0
        assert coordinator.data.device == original.device
        assert coordinator.data.system == original.system
        assert coordinator.data.state == original.state
        assert coordinator.data.batteries == original.batteries
        websocket.subscribe.assert_awaited_once_with(WebSocketTopic.ALL)

        # Even a manually requested refresh must not fetch via HTTP.
        await coordinator.async_refresh()
        api.combined.assert_not_awaited()
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    assert not coordinator._websocket_active
    assert coordinator.update_interval == UPDATE_INTERVAL


async def test_http_does_not_poll_while_websocket_active(
    hass: HomeAssistant,
    mock_config_entry_v2: MockConfigEntry,
    mock_homewizardenergy_v2: MagicMock,
) -> None:
    """Test a refresh does not overwrite P1 data or call HTTP during WSS."""
    mock_config_entry_v2.add_to_hass(hass)
    api = mock_homewizardenergy_v2
    original = api.combined.return_value
    coordinator = HWEnergyDeviceUpdateCoordinator(hass, mock_config_entry_v2, api)
    coordinator.data = replace(
        original,
        measurement=replace(original.measurement, power_w=1500.0),
    )
    coordinator._websocket_active = True
    coordinator.update_interval = None

    api.combined.return_value = replace(
        original,
        measurement=replace(original.measurement, power_w=900.0),
    )
    api.combined.reset_mock()

    result = await coordinator._async_update_data()

    assert result.measurement.power_w == 1500.0
    assert result.device == original.device
    assert result.system == original.system
    assert result.state == original.state
    assert result.batteries == original.batteries
    api.combined.assert_not_awaited()


async def test_http_fallback_when_websocket_disconnected(
    hass: HomeAssistant,
    mock_config_entry_v2: MockConfigEntry,
    mock_homewizardenergy_v2: MagicMock,
) -> None:
    """Test HTTP takes over when WSS is disconnected."""
    mock_config_entry_v2.add_to_hass(hass)
    api = mock_homewizardenergy_v2
    original = api.combined.return_value
    coordinator = HWEnergyDeviceUpdateCoordinator(hass, mock_config_entry_v2, api)
    coordinator.data = replace(
        original,
        measurement=replace(original.measurement, power_w=1500.0),
    )
    coordinator._websocket_active = False
    coordinator.update_interval = UPDATE_INTERVAL

    api.combined.return_value = replace(
        original,
        measurement=replace(original.measurement, power_w=900.0),
    )
    api.combined.reset_mock()

    result = await coordinator._async_update_data()

    assert result.measurement.power_w == 900.0
    assert result.device == original.device
    assert result.system == original.system
    assert result.state == original.state
    assert result.batteries == original.batteries
    assert coordinator.update_interval == UPDATE_INTERVAL
    api.combined.assert_awaited_once_with()


async def test_websocket_reconnect_resubscribes(
    hass: HomeAssistant,
    mock_config_entry_v2: MockConfigEntry,
    mock_homewizardenergy_v2: MagicMock,
) -> None:
    """Test HTTP fallback during disconnect and WSS takeover on reconnect."""
    mock_config_entry_v2.add_to_hass(hass)
    api = mock_homewizardenergy_v2
    original = api.combined.return_value
    coordinator = HWEnergyDeviceUpdateCoordinator(hass, mock_config_entry_v2, api)
    coordinator.async_set_updated_data(original)
    api.combined.reset_mock()

    first = replace(original.measurement, power_w=100.0)
    second = replace(original.measurement, power_w=200.0)
    second_received = asyncio.Event()
    reconnect_waiting = asyncio.Event()
    allow_reconnect = asyncio.Event()
    sleep_delays: list[float] = []

    async def first_events() -> AsyncIterator[SimpleNamespace]:
        yield SimpleNamespace(type=WebSocketTopic.MEASUREMENT, data=first)
        raise RequestError("Simulated WebSocket disconnect")

    async def second_events() -> AsyncIterator[SimpleNamespace]:
        yield SimpleNamespace(type=WebSocketTopic.MEASUREMENT, data=second)
        second_received.set()
        await asyncio.Future()

    first_context, first_ws = _mock_websocket(first_events())
    second_context, second_ws = _mock_websocket(second_events())
    api.websocket.side_effect = [first_context, second_context]

    async def wait_to_reconnect(delay: float) -> None:
        sleep_delays.append(delay)
        reconnect_waiting.set()
        await allow_reconnect.wait()

    with patch(
        "homeassistant.components.homewizard.coordinator.asyncio.sleep",
        new=wait_to_reconnect,
    ):
        task = asyncio.create_task(coordinator.async_process_websocket_events(api))
        try:
            await asyncio.wait_for(reconnect_waiting.wait(), timeout=5)
            assert not coordinator._websocket_active
            assert coordinator.update_interval == UPDATE_INTERVAL

            # During the reconnect delay, HTTP provides the latest readings.
            api.combined.return_value = replace(
                original,
                measurement=replace(original.measurement, power_w=900.0),
            )
            await coordinator.async_refresh()
            assert coordinator.data.measurement.power_w == 900.0
            api.combined.assert_awaited_once_with()

            allow_reconnect.set()
            await asyncio.wait_for(second_received.wait(), timeout=5)
            assert api.websocket.call_count == 2
            first_ws.subscribe.assert_awaited_once_with(WebSocketTopic.ALL)
            second_ws.subscribe.assert_awaited_once_with(WebSocketTopic.ALL)
            assert coordinator._websocket_active
            assert coordinator.update_interval is None
            assert coordinator.data.measurement.power_w == 200.0
            assert sleep_delays == [5]

            # HTTP must stop again after the new WSS connection takes over.
            api.combined.reset_mock()
            await coordinator.async_refresh()
            api.combined.assert_not_awaited()
        finally:
            allow_reconnect.set()
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task

    assert not coordinator._websocket_active
    assert coordinator.update_interval == UPDATE_INTERVAL


async def test_websocket_system_event(
    hass: HomeAssistant,
    mock_config_entry_v2: MockConfigEntry,
    mock_homewizardenergy_v2: MagicMock,
) -> None:
    """Test system information can be updated without HTTP polling."""
    mock_config_entry_v2.add_to_hass(hass)
    api = mock_homewizardenergy_v2
    original = api.combined.return_value
    assert original.system is not None

    coordinator = HWEnergyDeviceUpdateCoordinator(hass, mock_config_entry_v2, api)
    coordinator.async_set_updated_data(original)
    api.combined.reset_mock()

    system = replace(original.system, wifi_rssi_db=-60)
    received = asyncio.Event()

    async def events() -> AsyncIterator[SimpleNamespace]:
        yield SimpleNamespace(type=WebSocketTopic.SYSTEM, data=system)
        received.set()
        await asyncio.Future()

    context, websocket = _mock_websocket(events())
    api.websocket.return_value = context

    task = asyncio.create_task(coordinator.async_process_websocket_events(api))
    try:
        await asyncio.wait_for(received.wait(), timeout=5)
        websocket.subscribe.assert_awaited_once_with(WebSocketTopic.ALL)
        assert coordinator.data.system.wifi_rssi_db == -60
        assert coordinator.data.measurement == original.measurement
        assert coordinator.update_interval is None
        await coordinator.async_refresh()
        api.combined.assert_not_awaited()
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


async def test_websocket_waits_for_inflight_http_request(
    hass: HomeAssistant,
    mock_config_entry_v2: MockConfigEntry,
    mock_homewizardenergy_v2: MagicMock,
) -> None:
    """Test WSS takes over only after an in-flight HTTP request finishes."""
    mock_config_entry_v2.add_to_hass(hass)
    api = mock_homewizardenergy_v2
    original = api.combined.return_value
    coordinator = HWEnergyDeviceUpdateCoordinator(hass, mock_config_entry_v2, api)
    coordinator.async_set_updated_data(original)

    http_started = asyncio.Event()
    finish_http = asyncio.Event()
    subscribed = asyncio.Event()
    reading_events = asyncio.Event()

    async def delayed_http() -> CombinedModels:
        http_started.set()
        await finish_http.wait()
        return original

    async def subscribe(_topic: WebSocketTopic) -> None:
        subscribed.set()

    async def events() -> AsyncIterator[SimpleNamespace]:
        reading_events.set()
        await asyncio.Future()
        yield SimpleNamespace(
            type=WebSocketTopic.MEASUREMENT, data=original.measurement
        )

    context, websocket = _mock_websocket(events())
    websocket.subscribe.side_effect = subscribe
    api.websocket.return_value = context
    api.combined.side_effect = delayed_http
    api.combined.reset_mock()

    http_task = asyncio.create_task(coordinator._async_update_data())
    try:
        await asyncio.wait_for(http_started.wait(), timeout=5)
        ws_task = asyncio.create_task(coordinator.async_process_websocket_events(api))
        try:
            await asyncio.wait_for(subscribed.wait(), timeout=5)
            assert not coordinator._websocket_active

            finish_http.set()
            await asyncio.wait_for(http_task, timeout=5)
            await asyncio.wait_for(reading_events.wait(), timeout=5)
            assert coordinator._websocket_active
            assert coordinator.update_interval is None
            api.combined.assert_awaited_once_with()
        finally:
            ws_task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await ws_task
    finally:
        finish_http.set()
        if not http_task.done():
            http_task.cancel()
            await asyncio.gather(http_task, return_exceptions=True)
