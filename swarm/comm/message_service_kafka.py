# MIT License
#
# Copyright (c) 2024 swarm-workflows

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#
# Author: Komal Thareja(kthare10@renci.org)
import json
import logging
import threading
import time
import traceback
from abc import ABC, abstractmethod

from confluent_kafka import Producer, Consumer, TopicPartition

from swarm.comm.observer import Observer


class MessageServiceKafka:
    def __init__(self, config: dict, logger: logging.Logger):
        self.kafka_bootstrap_servers = config.get('kafka_bootstrap_servers')
        self.kafka_topic = config.get('kafka_topic')
        self.consumer_group_id = config.get('consumer_group_id')
        self.enable_auto_commit = config.get("enable_auto_commit", False)
        self.batch_size = config.get("batch_size", 10)
        self.producer = Producer({"bootstrap.servers": self.kafka_bootstrap_servers, "acks": "all",})
        self.consumer = Consumer({
            "bootstrap.servers": self.kafka_bootstrap_servers,
            "group.id": self.consumer_group_id,
            "auto.offset.reset": "latest",
            "enable.auto.commit": self.enable_auto_commit,
            "on_commit": self.commit_completed,
            "heartbeat.interval.ms": 1000
        })
        self.consumer.commit()
        self.logger = logger
        self.observers = []
        self.shutdown = False
        self.thread = threading.Thread(target=self.consume_messages, daemon=True,
                                       name=f"KafkaConsumer-{self.kafka_topic}")

    def start(self):
        self.consumer.subscribe([self.kafka_topic])
        self.thread.start()

    def stop(self):
        self.shutdown = True
        if self.thread.is_alive():
            self.thread.join()

    def register_observers(self, agent: Observer):
        if agent not in self.observers:
            self.observers.append(agent)

    def produce_message(self, json_message: dict, topic: str = None, src: int = None,
                        dest: int = None, fwd: int = None):
        message = json.dumps(json_message)
        outgoing_topic = self.kafka_topic
        if topic:
            outgoing_topic = topic
        msg_type = json_message.get("message_type")
        msg_name = None
        if msg_type:
            from swarm.comm.messages.message import MessageType
            msg_name = MessageType(msg_type)
        if fwd and src and dest:
            self.logger.debug(
                f"[OUTBOUND] [{str(msg_name)}] [SRC: {src}] [DEST: {dest}] [FWD: {fwd}] sent to "
                f"topic: {outgoing_topic}, Payload:  {message}")
        elif src and dest:
            self.logger.debug(
                f"[OUTBOUND] [{str(msg_name)}] [SRC: {src}] [DEST: {dest}] sent to topic: {outgoing_topic}, "
                f"Payload:  {message}")
        else:
            self.logger.debug(
                f"[OUTBOUND] [{str(msg_name)}] sent to topic: {outgoing_topic}, "
                f"Payload:  {message}")

        self.producer.produce(outgoing_topic, message.encode('utf-8'))
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
        #while not self.shutdown or lag:
        while not self.shutdown:
            try:
                msg = self.consumer.poll(0.25)
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

                #self.logger.debug(
                #    f"Partition {partition}: Current Offset={current_offset}, Highwater Mark={highwater_mark}, "
                #    f"Lag={lag} msg={msg.value().decode('utf-8')}")
                begin = time.time()
                self.notify_observers(msg=msg)
                #self.logger.debug(f"KAFKA PROCESS TIME: {time.time() - begin:.0f}")

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
        #else:
        #    self.logger.debug(f"KAFKA: Committed partition offsets: {partitions}")