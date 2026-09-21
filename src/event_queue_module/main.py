import asyncio

from viam.components.sensor import Sensor
from viam.module.module import Module
from viam.resource.registry import Registry, ResourceCreatorRegistration

from .sensor import QueueSensor


def _register() -> None:
    Registry.register_resource_creator(
        Sensor.API,
        QueueSensor.MODEL,
        ResourceCreatorRegistration(QueueSensor.new, QueueSensor.validate_config),
    )


async def main() -> None:
    _register()
    module = Module.from_args()
    module.add_model_from_registry(Sensor.API, QueueSensor.MODEL)
    await module.start()


if __name__ == "__main__":
    asyncio.run(main())
