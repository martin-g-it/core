"""Update coordinator for HomeWizard."""

import asyncio
from dataclasses import replace
from typing import override

from homewizard_energy import HomeWizardEnergy, HomeWizardEnergyV2, WebSocketTopic
from homewizard_energy.errors import DisabledError, RequestError, UnauthorizedError
from homewizard_energy.models import (
    Batteries,
    CombinedModels as DeviceResponseEntry,
    Device,
    Measurement,
    System,
)

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ConfigEntryAuthFailed
from homeassistant.helpers import issue_registry as ir
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed

from .const import (
    DOMAIN,
    ISSUE_BATTERY_MODE_CLOUD_DISABLED,
    LOGGER,
    UPDATE_INTERVAL,
    battery_mode_cloud_issue_id,
)

type HomeWizardConfigEntry = ConfigEntry[HWEnergyDeviceUpdateCoordinator]

WEBSOCKET_TIMEOUT = 30
WEBSOCKET_RECONNECT_DELAY = 5
WEBSOCKET_MAX_RECONNECT_DELAY = 30


class HWEnergyDeviceUpdateCoordinator(DataUpdateCoordinator[DeviceResponseEntry]):
    """Gather data for the energy device."""

    api: HomeWizardEnergy
    api_disabled: bool = False
    config_entry: HomeWizardConfigEntry

    def __init__(
        self,
        hass: HomeAssistant,
        config_entry: HomeWizardConfigEntry,
        api: HomeWizardEnergy,
    ) -> None:
        """Initialize update coordinator."""
        super().__init__(
            hass,
            LOGGER,
            config_entry=config_entry,
            name=DOMAIN,
            update_interval=UPDATE_INTERVAL,
        )
        self.api = api
        self._websocket_active = False
        self._transport_lock = asyncio.Lock()

    async def async_process_websocket_events(self, api: HomeWizardEnergyV2) -> None:
        """Use WebSocket when available and HTTP polling otherwise."""
        reconnect_delay = WEBSOCKET_RECONNECT_DELAY

        while True:
            cancelled = False
            try:
                async with api.websocket() as websocket:
                    await websocket.subscribe(WebSocketTopic.ALL)

                    # A request already in flight must finish before switching
                    # to WebSocket. Cancel the next scheduled HTTP refresh.
                    async with self._transport_lock:
                        self._websocket_active = True
                        self.update_interval = None
                        if self.data is not None:
                            self.async_set_updated_data(self.data)

                    LOGGER.debug(
                        "HomeWizard WebSocket connected for %s",
                        self.config_entry.title,
                    )

                    # A stream of non-measurement events must not hide a stale
                    # P1 measurement stream.
                    measurement_deadline = (
                        self.hass.loop.time() + WEBSOCKET_TIMEOUT
                    )
                    events = websocket.events_typed()

                    while True:
                        async with asyncio.timeout_at(measurement_deadline):
                            event = await anext(events)

                        if self.data is None:
                            continue

                        if (
                            event.type == WebSocketTopic.MEASUREMENT
                            and isinstance(event.data, Measurement)
                        ):
                            updated = replace(self.data, measurement=event.data)
                            measurement_deadline = (
                                self.hass.loop.time() + WEBSOCKET_TIMEOUT
                            )
                            reconnect_delay = WEBSOCKET_RECONNECT_DELAY
                        elif (
                            event.type == WebSocketTopic.DEVICE
                            and isinstance(event.data, Device)
                        ):
                            updated = replace(self.data, device=event.data)
                        elif (
                            event.type == WebSocketTopic.SYSTEM
                            and isinstance(event.data, System)
                        ):
                            updated = replace(self.data, system=event.data)
                        elif (
                            event.type == WebSocketTopic.BATTERIES
                            and isinstance(event.data, Batteries)
                        ):
                            updated = replace(self.data, batteries=event.data)
                        else:
                            continue

                        if event.type in (
                            WebSocketTopic.SYSTEM,
                            WebSocketTopic.BATTERIES,
                        ):
                            self._update_battery_mode_cloud_repair_issue(updated)

                        self.async_set_updated_data(updated)

            except asyncio.CancelledError:
                cancelled = True
                raise
            except UnauthorizedError:
                LOGGER.error(
                    "HomeWizard WebSocket authentication failed for %s",
                    self.config_entry.title,
                )
                return
            except (RequestError, OSError, TimeoutError) as err:
                LOGGER.debug(
                    "HomeWizard WebSocket disconnected for %s: %s",
                    self.config_entry.title,
                    err,
                )
            except StopAsyncIteration:
                LOGGER.debug(
                    "HomeWizard WebSocket event stream ended for %s",
                    self.config_entry.title,
                )
            except Exception:
                LOGGER.exception(
                    "Unexpected HomeWizard WebSocket error for %s",
                    self.config_entry.title,
                )
            finally:
                # Restore the existing HTTP poller only if WebSocket had
                # taken ownership of this device's updates.
                if self._websocket_active:
                    self._websocket_active = False
                    self.update_interval = UPDATE_INTERVAL
                    if self.data is not None and not cancelled:
                        self.async_set_updated_data(self.data)

            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(
                reconnect_delay * 2, WEBSOCKET_MAX_RECONNECT_DELAY
            )

    def _update_battery_mode_cloud_repair_issue(
        self, data: DeviceResponseEntry
    ) -> None:
        """Update repair issue for incompatible battery mode and cloud state."""
        battery_mode_cloud_issue_active = (
            data.batteries is not None
            and data.system is not None
            and data.batteries.mode == Batteries.Mode.PREDICTIVE.value
            and data.system.cloud_enabled is False
        )
        issue_id = battery_mode_cloud_issue_id(self.config_entry.entry_id)
        issue_exists = (
            ir.async_get(self.hass).async_get_issue(DOMAIN, issue_id) is not None
        )
        if battery_mode_cloud_issue_active and not issue_exists:
            ir.async_create_issue(
                self.hass,
                DOMAIN,
                issue_id,
                is_fixable=True,
                is_persistent=False,
                translation_key=ISSUE_BATTERY_MODE_CLOUD_DISABLED,
                severity=ir.IssueSeverity.WARNING,
                data={"entry_id": self.config_entry.entry_id},
            )
        elif not battery_mode_cloud_issue_active and issue_exists:
            ir.async_delete_issue(self.hass, DOMAIN, issue_id)

    @override
    async def _async_update_data(self) -> DeviceResponseEntry:
        """Fetch data only while HTTP is the active data source."""
        async with self._transport_lock:
            if self._websocket_active and self.data is not None:
                return self.data

            try:
                data = await self.api.combined()
            except RequestError as ex:
                raise UpdateFailed(
                    translation_domain=DOMAIN,
                    translation_key="communication_error",
                ) from ex
            except DisabledError as ex:
                if not self.api_disabled:
                    self.api_disabled = True
                    if self.data is not None:
                        self.hass.config_entries.async_schedule_reload(
                            self.config_entry.entry_id
                        )
                raise UpdateFailed(
                    translation_domain=DOMAIN,
                    translation_key="api_disabled",
                ) from ex
            except UnauthorizedError as ex:
                raise ConfigEntryAuthFailed from ex

            self.api_disabled = False
            self._update_battery_mode_cloud_repair_issue(data)
            self.data = data
            return data
