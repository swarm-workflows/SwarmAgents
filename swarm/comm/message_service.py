import enum
import json
import logging
import threading
import time
import traceback
from abc import ABC, abstractmethod

from confluent_kafka import Producer, Consumer, TopicPartition


class MessageType(enum.Enum):
    HeartBeat = enum.auto()   #1
    TaskStatus = enum.auto()  #2
    Proposal = enum.auto()    #3
    Prepare = enum.auto()     #4
    Commit = enum.auto()      #5

    def __repr__(self):
        return self.name

    def __str__(self):
        return self.name


class Observer(ABC):
    @abstractmethod
    def process_message(self, message):
        """
        Process incoming message
        :param message:
        :return:
        """


class MessageService:
    def __init__(self, config: dict, logger: logging.Logger):
        self.kafka_bootstrap_servers = config.get('kafka_bootstrap_servers')
        self.kafka_topic = config.get('kafka_topic')
        self.consumer_group_id = config.get('consumer_group_id')
        self.enable_auto_commit = config.get("enable_auto_commit", False)
        self.batch_size = config.get("batch_size", 10)
        self.producer = Producer({"bootstrap.servers": self.kafka_bootstrap_servers})
        self.consumer = Consumer({
            "bootstrap.servers": self.kafka_bootstrap_servers,
            "group.id": self.consumer_group_id,
            "auto.offset.reset": "latest",
            "enable.auto.commit": self.enable_auto_commit,
            "on_commit": self.commit_completed
        })
        self.consumer.commit()
        self.logger = logger
        self.observers = []
        self.shutdown = False
        self.thread = threading.Thread(target=self.consume_messages, daemon=True, name="KafkaConsumer")

    def start(self):
        self.consumer.subscribe([self.kafka_topic])
        self.thread.start()

    def stop(self):
        self.shutdown = True
        if self.thread.is_alive():
            self.thread.join()

    def register_observers(self, agent):
        if agent not in self.observers:
            self.observers.append(agent)

    def produce_message(self, json_message: dict):
        message = json.dumps(json_message)
        self.logger.debug(f"Message sent: {message}")
        self.producer.produce(self.kafka_topic, message.encode('utf-8'))
        #self.producer.flush()

    def notify_observers(self, msg):
        message = msg.value().decode('utf-8')
        for o in self.observers:
            o.process_message(message)

    def consume_messages(self):
        msg_count = 0
        lag = 1
        offsets = []
        logging.getLogger().info("Consumer thread started")
        while not self.shutdown or lag:
            try:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    lag = 0
                    continue
                if msg.error():
                    self.logger.info("Consumer error: {}".format(msg.error()))
                    continue

                # Retrieve current offset and high-water mark
                partition = msg.partition()
                topic = msg.topic()
                current_offset = msg.offset()
                offsets.append(TopicPartition(topic=topic, partition=partition, offset=current_offset + 1))
                low_mark, highwater_mark = self.consumer.get_watermark_offsets(TopicPartition(topic=topic,
                                                                                              partition=partition))
                # Calculate lag
                lag = highwater_mark - current_offset

                self.logger.debug(
                    f"Partition {partition}: Current Offset={current_offset}, Highwater Mark={highwater_mark}, "
                    f"Lag={lag} msg={msg.value().decode('utf-8')}")
                begin = time.time()
                self.notify_observers(msg=msg)
                self.logger.debug(f"KAFKA PROCESS TIME: {time.time() - begin:.0f}")

                if not self.enable_auto_commit:
                    msg_count += 1
                    if msg_count % self.batch_size == 0:
                        self.consumer.commit(offsets=offsets)
                        offsets.clear()
            except KeyboardInterrupt:
                break
            except Exception as e:
                self.logger.error(f"KAFKA: consumer error: {e}")
                self.logger.error(traceback.format_exc())

        self.logger.info("KAFKA: Shutting down consumer..")
        # Commit all the messages processed if any pending
        if len(offsets):
            self.consumer.commit(offsets=offsets, asynchronous=False)
        self.consumer.close()
        self.logger.info("KAFKA: Consumer Shutting down complete..")

    def commit_completed(self, err, partitions):
        if err:
            self.logger.error(f"KAFKA: commit failure: {err}")
        else:
            self.logger.debug(f"KAFKA: Committed partition offsets: {partitions}")