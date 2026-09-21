"""Bounded FIFO queue sensor. See README for design + wiring."""

import asyncio
import logging
from collections import deque
from collections.abc import Mapping, Sequence
from typing import Any, ClassVar

from viam.components.sensor import Sensor
from viam.errors import NoCaptureToStoreError
from viam.proto.app.robot import ComponentConfig
from viam.proto.common import ResourceName
from viam.resource.base import ResourceBase
from viam.resource.types import Model, ModelFamily
from viam.utils import from_dm_from_extra, struct_to_dict

LOGGER = logging.getLogger(__name__)

DEFAULT_QUEUE_CAPACITY = 1000


class QueueSensor(Sensor):
    MODEL: ClassVar[Model] = Model(ModelFamily("viam", "event-queue"), "sensor")

    def __init__(self, name: str) -> None:
        super().__init__(name)
        self._queue: deque = deque(maxlen=DEFAULT_QUEUE_CAPACITY)
        self._capacity: int = DEFAULT_QUEUE_CAPACITY
        self._lock: asyncio.Lock | None = None
        self._dropped_total: int = 0
        self._pushed_total: int = 0

    @classmethod
    def new(
        cls,
        config: ComponentConfig,
        dependencies: Mapping[ResourceName, ResourceBase],
    ) -> "QueueSensor":
        s = cls(config.name)
        s.reconfigure(config, dependencies)
        return s

    @classmethod
    def validate_config(cls, config: ComponentConfig) -> Sequence[str]:
        attrs = struct_to_dict(config.attributes)
        capacity = attrs.get("queue_capacity")
        if capacity is not None:
            if not isinstance(capacity, int | float) or isinstance(capacity, bool):
                raise ValueError("`queue_capacity` must be a positive integer")
            if int(capacity) != capacity or capacity <= 0:
                raise ValueError("`queue_capacity` must be a positive integer")
        return []

    def reconfigure(
        self,
        config: ComponentConfig,
        dependencies: Mapping[ResourceName, ResourceBase],
    ) -> None:
        attrs = struct_to_dict(config.attributes)
        self._capacity = int(attrs.get("queue_capacity") or DEFAULT_QUEUE_CAPACITY)
        # Preserve in-flight items across reconfigure so a config change doesn't drop unsent events.
        self._queue = deque(self._queue, maxlen=self._capacity)
        self._lock = asyncio.Lock()

    async def get_readings(
        self,
        *,
        extra: Mapping[str, Any] | None = None,
        timeout: float | None = None,
        **kwargs: Any,
    ) -> Mapping[str, Any]:
        assert self._lock is not None
        from_dm = from_dm_from_extra(dict(extra) if extra else None)
        async with self._lock:
            if from_dm:
                if not self._queue:
                    raise NoCaptureToStoreError()
                return self._queue.popleft()
            return dict(self._queue[-1]) if self._queue else {}

    async def do_command(
        self,
        command: Mapping[str, Any],
        *,
        timeout: float | None = None,
        **kwargs: Any,
    ) -> Mapping[str, Any]:
        verb = command.get("command")
        if verb == "push_event":
            return await self._push(command.get("event"))
        raise ValueError(f"unknown command: {verb!r}")

    async def _push(self, event: Any) -> dict:
        if not isinstance(event, dict):
            raise ValueError("`event` must be an object")
        assert self._lock is not None
        async with self._lock:
            was_full = len(self._queue) == self._capacity
            self._queue.append(dict(event))
            self._pushed_total += 1
            if was_full:
                self._dropped_total += 1
            return {
                "ok": True,
                "queue_length": len(self._queue),
                "pushed_total": self._pushed_total,
                "dropped_total": self._dropped_total,
            }
