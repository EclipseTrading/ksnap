"""Read topic messages from kafka
"""
import logging
from abc import ABC, abstractmethod
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any, Dict, List, Set, Tuple

from confluent_kafka import Consumer, KafkaException, TopicPartition

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
        self.topics: List[str] = []

    @staticmethod
    def _check_timeout(timeout: int, start_time: datetime) -> bool:
        if not timeout:
            return False
        timeout_td = timedelta(seconds=timeout)
        current_td = datetime.now() - start_time
        return current_td > timeout_td

    @staticmethod
    def _check_reach_offsets(
            msg, offset_dict: Dict[Tuple[str, int], int]) -> bool:
        if (msg.topic(), msg.partition()) not in offset_dict:
            return True
        return offset_dict[(msg.topic(), msg.partition())] <= msg.offset()

    def subscribe(self, topics: List[str]):
        # TODO: consider having add_topics as methods
        self.topics = topics
        self.consumer.subscribe(
            topics,
        )

    def _get_latest_offsets(self) -> Dict[Tuple[str, int], int]:
        tps = []
        broker_topics = self.consumer.list_topics().topics
        for topic_name in self.topics:
            tps.extend([TopicPartition(topic_name, k)
                        for k in broker_topics[topic_name].partitions])
        d = {}
        for tp in tps:
            low, high = self.consumer.get_watermark_offsets(tp)
            if low == high:
                logger.info(f'No messages in topic: {tp.topic} '
                            f'partition: {tp.partition}')
                continue
            # high watermark is latest offset + 1
            d[(tp.topic, tp.partition)] = high - 1
            logger.debug(f'Latest offset for topic: {tp.topic} '
                         f'partition: {tp.partition}: {high - 1}')
        return d

    @staticmethod
    def generate_consumer_report(offset_dict, msg_dict, done_partitions):
        logger.debug(f'Done consuming from partitions:')
        for topic, partition in done_partitions:
            logger.debug(f'\t- topic: {topic} partition: {partition}')
        for (topic, partition), offset in offset_dict.items():
            if (topic, partition) in done_partitions:
                continue
            if (topic, partition) not in msg_dict:
                logger.warning(f'Did not consume from topic: {topic} '
                               f'partition: {partition}')
                continue
            last_read_offset = msg_dict[(topic, partition)][-1].offset
            logger.warning(f'Remaining messages for consumption from topic: '
                           f'{topic} partition: {partition}: '
                           f'{offset - last_read_offset}')

    def read(self, timeout: int = 0) -> Dict[Tuple[str, int], List[Message]]:
        msg_count = 0
        offset_dict = self._get_latest_offsets()
        done_partitions: Set[Tuple[str, int]] = set()
        msg_dict: Dict[Tuple[str, int], List[Any]] = defaultdict(list)
        try:
            start_time = datetime.now()
            while True:
                # break if timeout is reached
                if ConfluentKafkaReader._check_timeout(timeout, start_time):
                    logger.info(
                        f'Reached timeout: {timeout}s for reading messages.')
                    break
                # break if all partitions are marked as done
                if len(done_partitions) == len(offset_dict):
                    logger.info('Done consuming from '
                                f'{len(done_partitions)} partitions.')
                    break
                
                msgs = self.consumer.consume(num_messages=1000, timeout=60)
                for msg in msgs:
                    if msg is None:
                        continue
                    if msg.error():
                        raise KafkaException(msg.error())
                    # skip if partitions are marked as done
                    if (msg.topic(), msg.partition()) in done_partitions:
                        continue
                    # skip messages over required offsets
                    if ConfluentKafkaReader._check_reach_offsets(
                            msg, offset_dict):
                        logger.info(f'Done consuming from topic: '
                                    f'{msg.topic()} partition: '
                                    f'{msg.partition()}')
                        done_partitions.add((msg.topic(), msg.partition()))
                    message = Message(msg.offset(), msg.key(), msg.value(),
                                    msg.timestamp()[1], msg.headers())
                    msg_dict[(msg.topic(), msg.partition())].append(message)
                    msg_count += 1
                    if not msg_count % 100000:
                        logger.debug(
                            f"So far read {msg_count} messages from kafka"
                        )
        except KeyboardInterrupt:
            logger.info("%% Aborted by user\n")
        finally:
            self.close()
        logger.info("Done with reading")
        ConfluentKafkaReader.generate_consumer_report(
            offset_dict, msg_dict, done_partitions)
        return msg_dict

    def close(self):
        self.consumer.close()
