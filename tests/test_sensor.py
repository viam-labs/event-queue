import asyncio

import pytest

from viam.errors import NoCaptureToStoreError

from event_queue_module.sensor import DEFAULT_QUEUE_CAPACITY, QueueSensor


def _make(capacity: int = DEFAULT_QUEUE_CAPACITY) -> QueueSensor:
    s = QueueSensor(name="test")
    s._capacity = capacity
    from collections import deque

    s._queue = deque(maxlen=capacity)
    s._lock = asyncio.Lock()
    return s


async def test_push_then_drain_returns_oldest_first():
    s = _make()
    await s.do_command({"command": "push_event", "event": {"n": 1}})
    await s.do_command({"command": "push_event", "event": {"n": 2}})
    await s.do_command({"command": "push_event", "event": {"n": 3}})

    dm_extra = {"fromDataManagement": True}
    assert (await s.get_readings(extra=dm_extra))["n"] == 1
    assert (await s.get_readings(extra=dm_extra))["n"] == 2
    assert (await s.get_readings(extra=dm_extra))["n"] == 3


async def test_dm_call_on_empty_queue_raises_no_capture():
    s = _make()
    with pytest.raises(NoCaptureToStoreError):
        await s.get_readings(extra={"fromDataManagement": True})


async def test_non_dm_call_peeks_newest_without_draining():
    s = _make()
    await s.do_command({"command": "push_event", "event": {"n": 1}})
    await s.do_command({"command": "push_event", "event": {"n": 2}})

    assert (await s.get_readings())["n"] == 2
    assert (await s.get_readings())["n"] == 2
    assert (await s.get_readings(extra={"fromDataManagement": True}))["n"] == 1


async def test_non_dm_call_on_empty_queue_returns_empty_map():
    s = _make()
    assert await s.get_readings() == {}


async def test_capacity_drops_oldest_and_counts():
    s = _make(capacity=2)
    r1 = await s.do_command({"command": "push_event", "event": {"n": 1}})
    r2 = await s.do_command({"command": "push_event", "event": {"n": 2}})
    r3 = await s.do_command({"command": "push_event", "event": {"n": 3}})

    assert r1["dropped_total"] == 0
    assert r2["dropped_total"] == 0
    assert r3["dropped_total"] == 1
    assert r3["queue_length"] == 2

    assert (await s.get_readings(extra={"fromDataManagement": True}))["n"] == 2


async def test_push_rejects_non_object_event():
    s = _make()
    with pytest.raises(ValueError):
        await s.do_command({"command": "push_event", "event": "not a dict"})
    with pytest.raises(ValueError):
        await s.do_command({"command": "push_event"})


async def test_unknown_command_raises():
    s = _make()
    with pytest.raises(ValueError):
        await s.do_command({"command": "pop_all"})


async def test_push_returns_running_totals():
    s = _make()
    r1 = await s.do_command({"command": "push_event", "event": {"n": 1}})
    r2 = await s.do_command({"command": "push_event", "event": {"n": 2}})
    assert r1["pushed_total"] == 1
    assert r2["pushed_total"] == 2
    assert r2["queue_length"] == 2
