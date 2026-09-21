# event-queue

Bounded FIFO queue sensor for Viam. Producers push arbitrary JSON-shaped items
in via a DoCommand; the data manager drains one item per capture cycle into
tabular data. Downstream consumers (Triggers, dashboards, custom queries) then
see one row per pushed event.

The queue is in-memory and bounded. It does not persist across module restarts.

## Sensor

**Model:** `viam:event-queue:sensor`
**API:** `rdk:component:sensor`

### Configuration

```json
{
  "name": "my-events",
  "namespace": "rdk",
  "type": "sensor",
  "model": "viam:event-queue:sensor",
  "attributes": {
    "queue_capacity": 1000
  },
  "service_configs": [
    {
      "type": "data_manager",
      "attributes": {
        "capture_methods": [
          {
            "method": "Readings",
            "capture_frequency_hz": 1,
            "disabled": false,
            "additional_params": {}
          }
        ]
      }
    }
  ]
}
```

| Attribute        | Type | Default | Description                                                                                     |
|------------------|------|---------|-------------------------------------------------------------------------------------------------|
| `queue_capacity` | int  | `1000`  | Maximum queued items. Overflow drops the oldest item so a runaway producer can't grow memory.   |

### Producing events

Any Viam component or SDK client can push an event by resolving the sensor and
calling `do_command`:

```python
await queue_sensor.do_command({
    "command": "push_event",
    "event": {
        "event_type": "fed",
        "source": "feeder",
        "cups": 1.375,
        "at": "2026-09-21T20:03:00Z",
    },
})
```

The `event` payload is stored as-is; any JSON-shaped dict is accepted. The
returned map reports the running `queue_length`, `pushed_total`, and
`dropped_total` counters.

### Draining

- **When data manager calls `get_readings`**, the sensor pops the *oldest* item
  and returns it as that capture's row. When the queue is empty it raises
  `NoCaptureToStoreError`, so empty captures are skipped rather than stored as
  null rows.
- **When any other caller reads `get_readings`** (UI, curl, health check), the
  sensor returns the *newest* item non-destructively. If the queue is empty,
  an empty map is returned.

### Wiring up a Trigger

Set capture frequency on `Readings` fast enough that events don't backlog
(1 Hz is fine for most home setups; higher for burstier producers). Configure a
Viam Trigger with `conditional_data_ingested`:

```json
{
  "name": "on-feed",
  "event": {
    "type": "conditional_data_ingested",
    "conditional": {
      "data_capture_method": "sensor:my-events:Readings",
      "condition": {
        "evals": [
          { "operator": "eq", "value": { "event_type": "fed" } }
        ]
      }
    }
  },
  "notifications": [
    { "type": "email", "value": "5551234567@tmomail.net", "seconds_between_notifications": 0 }
  ]
}
```

## Development

```
python3 -m venv .venv
./.venv/bin/pip install -e '.[dev]'
./.venv/bin/pytest
```
