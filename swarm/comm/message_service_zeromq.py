import asyncio
import json
import logging
import threading
import traceback
from nats.aio.client import Client as NATS

from swarm.comm.observer import Observer


import json
import logging
import threading
import traceback
import zmq


class MessageServiceZeroMQ:
    def __init__(self, config: dict, logger: logging.Logger):
        self.zeromq_pub_url = config.get('zeromq_pub_url')  # e.g., "tcp://127.0.0.1:5555"
        self.zeromq_sub_url = config.get('zeromq_sub_url')  # e.g., "tcp://127.0.0.1:5556"
        self.topic = config.get('zeromq_topic', 'default_topic')
        self.logger = logger
        self.observers = []
        self.shutdown = False

        # ZeroMQ Context
        self.context = zmq.Context()

        # ZeroMQ Publisher socket
        self.publisher_socket = self.context.socket(zmq.PUB)
        self.publisher_socket.bind(self.zeromq_pub_url)

        # ZeroMQ Subscriber socket
        self.subscriber_socket = self.context.socket(zmq.SUB)
        self.subscriber_socket.connect(self.zeromq_sub_url)
        self.subscriber_socket.setsockopt_string(zmq.SUBSCRIBE, self.topic)

        # Consumer thread to receive messages
        self.thread = threading.Thread(target=self.consume_messages, daemon=True, name="ZeroMQConsumer")

    def start(self):
        self.thread.start()

    def stop(self):
        self.shutdown = True
        if self.thread.is_alive():
            self.thread.join()

    def register_observers(self, agent):
        if agent not in self.observers:
            self.observers.append(agent)

    def produce_message(self, json_message: dict):
        """
        Publish a message using ZeroMQ PUB socket.
        :param json_message: The message to publish
        """
        try:
            message = json.dumps(json_message)
            # The message will be prefixed with the topic
            self.publisher_socket.send_string(f"{self.topic} {message}")
            self.logger.debug(f"Message sent: {message} to topic: {self.topic}")
        except Exception as e:
            self.logger.error(f"ZeroMQ: Failed to publish message: {e}")
            self.logger.error(traceback.format_exc())

    def notify_observers(self, message):
        """
        Notify all observers with the new message.
        :param message: The message received from the topic
        """
        for observer in self.observers:
            observer.process_message(message)

    def consume_messages(self):
        """
        Consume messages from the ZeroMQ SUB socket.
        """
        logging.getLogger().info("ZeroMQ Consumer thread started")
        while not self.shutdown:
            try:
                # Receive the message (blocking call)
                message = self.subscriber_socket.recv_string()
                topic, message_data = message.split(" ", 1)  # Split the topic and message

                if topic == self.topic:
                    self.notify_observers(message_data)

            except Exception as e:
                self.logger.error(f"ZeroMQ: consumer error: {e}")
                self.logger.error(traceback.format_exc())

        self.logger.info("ZeroMQ: Shutting down consumer..")
        self.subscriber_socket.close()
        self.logger.info("ZeroMQ: Consumer shutdown complete.")
