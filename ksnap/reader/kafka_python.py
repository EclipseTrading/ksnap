"""Read topic messages from kafka
"""
import logging
from typing import Any, Dict, List, Set, Tuple
from collections import defaultdict
from datetime import datetime

from kafka.consumer.fetcher import ConsumerRecord
from kafka import KafkaConsumer, TopicPartition
from ksnap.message import Message
from ksnap.reader.base import KafkaReader

logger = logging.getLogger(__name__)


class PythonKafkaReader(KafkaReader):

    def __init__(self, kafka_hosts):
        self.config = {
                "bootstrap_servers": kafka_hosts,
                "client_id": "KsnapClient",
                "max_poll_interval_ms": 10000,
                "auto_offset_reset": "earliest",
                "enable_auto_commit": False,
            }
        self.consumer = KafkaConsumer(**self.config)
        self.topics: List[str] = []

    @staticmethod
    def _check_reach_offsets(msg: ConsumerRecord, offset_dict):
        if (msg.topic, msg.partition) not in offset_dict:
            return True
        return offset_dict[(msg.topic, msg.partition)] <= msg.offset

    def list_topics(self) -> Set[str]:
        return self.consumer.topics()

    def subscribe(self, topics: List[str]):
        # TODO: consider having add_topics as methods
        self.topics = topics
        tps = []
        for t in self.consumer.topics():
            if t not in topics:
                continue
            for p in self.consumer.partitions_for_topic(t):
                tps.append(TopicPartition(t, p))
        self.consumer.assign(tps)

    def _get_latest_offsets(self) -> Dict[Tuple[str, int], int]:
        tps: List[TopicPartition] = []
        for t in self.consumer.topics():
            if t not in self.topics:
                continue
            partitions = self.consumer.partitions_for_topic(t)
            for p in partitions:
                tps.append(TopicPartition(t, p))
        d = {}
        low_offset_dict = self.consumer.beginning_offsets(tps)
        high_offset_dict = self.consumer.end_offsets(tps)

        for tp in tps:
            low = low_offset_dict.get(tp)
            high = high_offset_dict.get(tp)
            if high is None:
                logger.debug(tp)
                continue
            if low == high:
                logger.info(f'No messages in topic: {tp.topic} '
                            f'partition: {tp.partition}')
                continue
            # high watermark is latest offset + 1
            d[(tp.topic, tp.partition)] = high - 1
            logger.debug(f'Latest offset for topic: {tp.topic} '
                         f'partition: {tp.partition}: {high - 1}')
        return d

    def read(self, timeout: int = 0) -> Dict[Tuple[str, int], List[Message]]:
        msg_count = 0
        offset_dict = self._get_latest_offsets()
        done_partitions: Set[Tuple[str, int]] = set()
        msg_dict: Dict[Tuple[str, int], List[Any]] = defaultdict(list)
        try:
            start_time = datetime.now()
            while True:
                # break if timeout is reached
                if PythonKafkaReader._check_timeout(timeout, start_time):
                    logger.info(
                        f'Reached timeout: {timeout}s for reading messages.')
                    break
                # break if all partitions are marked as done
                if len(done_partitions) == len(offset_dict):
                    logger.info('Done consuming from '
                                f'{len(done_partitions)} partitions.')
                    break
                msg: ConsumerRecord = next(self.consumer)
                if msg is None:
                    continue
                # skip if partitions are marked as done
                if (msg.topic, msg.partition) in done_partitions:
                    continue
                # skip messages over required offsets
                if PythonKafkaReader._check_reach_offsets(
                        msg, offset_dict):
                    logger.info(f'Done consuming from topic: '
                                f'{msg.topic} partition: '
                                f'{msg.partition}')
                    self.consumer.pause(
                        TopicPartition(msg.topic, msg.partition))
                    done_partitions.add((msg.topic, msg.partition))
                message = Message(msg.offset, msg.key, msg.value,
                                  msg.timestamp, msg.headers)
                msg_dict[(msg.topic, msg.partition)].append(message)
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
        PythonKafkaReader.generate_consumer_report(
            offset_dict, msg_dict, done_partitions)
        return msg_dict

    def close(self):
        self.consumer.close(autocommit=False)
