"""Read topic messages from kafka
"""
import logging
from abc import ABC, abstractmethod
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any, Dict, List, Tuple

from confluent_kafka import Consumer, KafkaException

from ksnap.message import Message

logger = logging.getLogger(__name__)


class KafkaReader(ABC):
    @abstractmethod
    def subscribe(self, topics: List[str]):
        pass

    @abstractmethod
    def read(self):
        pass

    @abstractmethod
    def close(self):
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

    def subscribe(self, topics: List[str]):
        self.consumer.subscribe(
            topics,
        )

    def read(self, timeout: int = 0) -> Dict[Tuple[str, int], List[Message]]:
        msg_count = 0
        msg_dict: Dict[Tuple[str, int], List[Any]] = defaultdict(list)
        try:
            counter = 0
            start_time = datetime.now()
            while counter < 2:
                msg = self.consumer.poll(timeout=5.0)
                if msg is None:
                    counter += 1
                    continue
                counter = 0
                if msg.error():
                    raise KafkaException(msg.error())
                message = Message(msg.offset(), msg.key(), msg.value(),
                                  msg.timestamp()[1], msg.headers())
                msg_dict[(msg.topic(), msg.partition())].append(message)
                msg_count += 1
                if not msg_count % 100000:
                    logger.debug(
                        f"So far read {msg_count} messages from kafka"
                    )
                if datetime.now() - start_time > timedelta(seconds=timeout):
                    logger.info(
                        f'Reached timeout: {timeout}s for reading messages.')
                    break
            logger.info("Done with reading")
        except KeyboardInterrupt:
            logger.info("%% Aborted by user\n")
        finally:
            self.consumer.close()
        return msg_dict

    def close(self):
        self.consumer.close()
