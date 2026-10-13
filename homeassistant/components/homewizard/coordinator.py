"""Update coordinator for HomeWizard."""

import asyncio
from dataclasses import replace
from typing import override

from homewizard_energy import HomeWizardEnergy, HomeWizardEnergyV2, WebSocketTopic
from homewizard_energy.errors import DisabledError, RequestError, UnauthorizedError
from homewizard_energy.models import (
    Batteries,
    CombinedModels as DeviceResponseEntry,
    Measurement,
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

    async def async_process_websocket_events(
        self,
        api: HomeWizardEnergyV2,
    ) -> None:
        """Process WebSocket measurements with automatic reconnection."""
        reconnect_delay = WEBSOCKET_RECONNECT_DELAY

        try:
            while True:
                try:
                    async with api.websocket() as websocket:
                        await websocket.subscribe(WebSocketTopic.MEASUREMENT)

                        LOGGER.debug(
                            "HomeWizard WebSocket connected for %s",
                            self.config_entry.title,
                        )

                        events = websocket.events_typed()

                        while True:
                            async with asyncio.timeout(WEBSOCKET_TIMEOUT):
                                event = await anext(events)

                            if (
                                event.type != WebSocketTopic.MEASUREMENT
                                or not isinstance(event.data, Measurement)
                                or self.data is None
                            ):
                                continue

                            self._websocket_active = True
                            reconnect_delay = WEBSOCKET_RECONNECT_DELAY

                            updated_data = replace(
                                self.data,
                                measurement=event.data,
                            )
                            self.async_set_updated_data(updated_data)

                except asyncio.CancelledError:
                    raise

                except UnauthorizedError:
                    LOGGER.error(
                        "HomeWizard WebSocket authentication failed for %s",
                        self.config_entry.title,
                    )
                    return

                except (RequestError, OSError, TimeoutError) as err:
                    LOGGER.warning(
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
                    self._websocket_active = False

                LOGGER.debug(
                    "Reconnecting HomeWizard WebSocket for %s in %s seconds",
                    self.config_entry.title,
                    reconnect_delay,
                )

                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(
                    reconnect_delay * 2,
                    WEBSOCKET_MAX_RECONNECT_DELAY,
                )

        finally:
            self._websocket_active = False

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
        """Fetch all device and sensor data from api."""
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

                # Do not reload when performing first refresh
                if self.data is not None:
                    # Reload config entry to let init flow handle
                    # retrying and trigger repair flow
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

        # Preserve the latest WebSocket measurement while HTTP refreshes
        # the remaining device, state, system and battery information.
        if self._websocket_active and self.data is not None:
            data = replace(
                data,
                measurement=self.data.measurement,
            )

        self._update_battery_mode_cloud_repair_issue(data)

        self.data = data
        return data
