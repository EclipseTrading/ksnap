"""Write topic messages to kafka
"""
from ksnap.message import Message
import logging
from abc import ABC, abstractmethod
from typing import List

from confluent_kafka import Producer


logger = logging.getLogger(__name__)


class KafkaWriter(ABC):
    @abstractmethod
    def write(self, topic: str, partiton: int, message: Message):
        pass

    @abstractmethod
    def close(self):
        pass


class ConfluentKafkaWriter(KafkaWriter):

    def __init__(self, kafka_hosts: List[str]):
        # TODO: adjust
        self.config = {
            'bootstrap.servers': ','.join(kafka_hosts),
            'queue.buffering.max.messages': 50000000,
            'queue.buffering.max.ms': 900000,
            'batch.num.messages': 10,
            'message.timeout.ms': 2147483647,
            'queue.buffering.max.kbytes': 2097151,
        }
        self.producer = Producer(**self.config)
        self.messages: List[Message] = []

    def write(self, topic: str, partiton: int, message: Message):
        while True:
            try:
                self.producer.produce(
                    topic=topic, value=message.value, key=message.key,
                    partition=partiton, on_delivery=self.delivery_callback,
                    timestamp=message.timestamp,
                    headers=message.headers)
                self.producer.poll(0)
                break
            except BufferError as e:
                print(e, file=sys.stderr)
                self.producer.poll(100)

    def flush(self):
        self.producer.flush()

    def delivery_callback(self, err, msg):
        if err:
            logger.error('%% Message failed delivery: %s\n' % err)
        else:
            self.messages.append(Message(msg.offset(), msg.key(), msg.value(),
                                         msg.timestamp()[1], None))

    def close(self):
        self.producer.close()
