import asyncio
import json
import logging
import threading
import traceback
from nats.aio.client import Client as NATS
from nats.aio.errors import ErrConnectionClosed, ErrTimeout

from swarm.comm.observer import Observer


class MessageServiceNats:
    def __init__(self, config: dict, logger: logging.Logger):
        self.nats_servers = config.get('nats_servers', 'nats://127.0.0.1:4222')
        self.nats_topic = config.get('nats_topic', 'job.consensus')
        self.logger = logger
        self.batch_size = config.get("batch_size", 10)
        self.shutdown = False
        self.observers = []
        self.offsets = []
        self.consumer_thread = threading.Thread(target=self._start_consumer, daemon=True, name="NATSConsumer")
        self.loop = None

    async def _connect(self):
        self.nc = NATS()
        await self.nc.connect(servers=[self.nats_servers])
        self.logger.info("Connected to NATS server")

    def start(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self.loop.run_until_complete(self._connect())
        self.consumer_thread.start()

    def stop(self):
        self.shutdown = True
        if self.consumer_thread.is_alive():
            self.consumer_thread.join()

    def register_observers(self, agent: Observer):
        if agent not in self.observers:
            self.observers.append(agent)

    async def produce_message(self, json_message: dict):
        message = json.dumps(json_message)
        try:
            await self.nc.publish(self.nats_topic, message.encode('utf-8'))
            await self.nc.flush()
            self.logger.debug(f"Message sent: {message} to topic: {self.nats_topic}")
        except Exception as e:
            self.logger.error(f"NATS: Failed to publish message: {e}")
            self.logger.error(traceback.format_exc())

    def notify_observers(self, message):
        decoded_message = message.decode('utf-8')
        for observer in self.observers:
            observer.process_message(decoded_message)

    async def consume_messages(self):
        async def message_handler(msg):
            self.logger.debug(f"Received message on {msg.subject}: {msg.data.decode()}")
            self.notify_observers(msg.data)

        await self.nc.subscribe(self.nats_topic, cb=message_handler)

        # Keep the subscriber running
        while not self.shutdown:
            try:
                await asyncio.sleep(0.25)  # Small sleep to avoid CPU overload
            except Exception as e:
                self.logger.error(f"NATS: consumer error: {e}")
                self.logger.error(traceback.format_exc())

    def _start_consumer(self):
        if self.loop is None:
            self.logger.error("NATS: Event loop is not initialized")
            return
        asyncio.set_event_loop(self.loop)
        self.loop.run_until_complete(self.consume_messages())

# Example usage:
# config = {
#     'nats_servers': 'nats://127.0.0.1:4222',
#     'nats_topic': 'job.consensus',
#     'batch_size': 10,
# }
# logger = logging.getLogger("NATSMessageService")
# message_service = MessageService(config=config, logger=logger)
# message_service.start()
