# Read topic messages from kafka
import logging
from abc import ABC, abstractmethod
from collections import defaultdict
from typing import List

from confluent_kafka import Consumer, KafkaException

logger = logging.getLogger(__name__)


class KafkaReader(ABC):
    @abstractmethod
    def subscribe():
        pass

    @abstractmethod
    def read():
        pass

    @abstractmethod
    def close():
        pass


class ConfluentKafkaReader(KafkaReader):
    def __init__(self, kafka_hosts: List[str]):
        self.config = {
            "bootstrap.servers": ",".join(kafka_hosts),
            "group.id": "TestConnectivityClient2",
            "max.poll.interval.ms": 10000,
            "auto.offset.reset": "smallest",
            "enable.auto.commit": False,
        }
        self.consumer = Consumer(self.config, logger=logger)

    def subscribe(self, topics):
        self.consumer.subscribe(
            topics,
        )

    def read(self):
        msg_count = 0
        msg_dict = defaultdict(list)
        try:
            counter = 0
            while True and counter < 5:
                msg = self.consumer.poll(timeout=5.0)
                if msg is None:
                    counter += 1
                    continue
                if msg.error():
                    raise KafkaException(msg.error())
                msg_dict[(msg.topic(), msg.partition())].append(
                    (msg.offset(), msg.key(), msg.value())
                )
                msg_count += 1
                if not msg_count % 100000:
                    logger.debug(f"Read {msg_count} messages so far.")
            return msg_dict
        except KeyboardInterrupt:
            logger.info("%% Aborted by user\n")
        finally:
            self.close()

    def close(self):
        self.consumer.close()
