"""Kafka admin client for consumer group related operations.
"""
import logging
from abc import abstractmethod
from collections import defaultdict
from typing import DefaultDict, Dict, List
from multiprocessing.pool import ThreadPool
import re

from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient

from ksnap.offset import Offset

IGNORE_GROUP_REGEX = '[a-f0-9]{8}(-[a-f0-9]{4}){4}[a-f0-9]{8}'
logger = logging.getLogger(__name__)


class KafkaAdmin:

    def __init__(self, kafka_hosts: List[str]):
        pass

    @abstractmethod
    def get_consumer_groups(self) -> List[str]:
        pass

    @abstractmethod
    def get_consumer_offsets(self) -> List[Offset]:
        pass


class ConfluentAdminClient:

    def __init__(self, kafka_hosts: List[str]):
        self.config = {
            "bootstrap.servers": ",".join(kafka_hosts),
        }
        self.client = AdminClient(self.config)

    @staticmethod
    def group_offsets_by_consumer_group(
            offsets: List[Offset]) -> Dict[str, List[Offset]]:
        d: DefaultDict[str, List[Offset]] = defaultdict(list)
        for offset in offsets:
            d[offset.consumer_group].append(offset)
        return d

    def get_consumer_groups(self) -> List[str]:
        consumer_groups = self.client.list_groups()
        return [g.id for g in consumer_groups]

    def get_consumer_offsets(
        self, topics: List[str], ignore_group_regex: str = IGNORE_GROUP_REGEX,
        no_of_threads: int = 8
            ) -> List[Offset]:
        broker_topics = self.client.list_topics().topics
        partitions = []
        for topic_name in topics:
            partitions.extend([TopicPartition(topic_name, k)
                               for k in broker_topics[topic_name].partitions])
        consumer_groups = []
        for consumer_group in self.get_consumer_groups():
            if re.findall(ignore_group_regex, consumer_group):
                logger.debug(f'Ignoring consumer group: {consumer_group}')
                continue
            consumer_groups.append(consumer_group)

        def func(consumer_group) -> List[Offset]:
            offsets = []
            consumer = Consumer({**self.config, 'group.id': consumer_group})
            for tp in consumer.committed(partitions, timeout=10):
                if tp.offset == -1001:
                    continue
                offset = Offset(consumer_group, tp.topic,
                                tp.partition, tp.offset)
                offsets.append(offset)
            return offsets
        pool = ThreadPool(no_of_threads)
        offsets: List[Offset] = []
        for _offsets in pool.map(func, consumer_groups):
            offsets.extend(_offsets)
        return offsets

    def set_consumer_offsets(self, offsets: List[Offset]):
        grouped_offsets = ConfluentAdminClient.group_offsets_by_consumer_group(
            offsets)
        for consumer_group, _offsets in grouped_offsets.items():
            consumer = Consumer({**self.config, 'group.id': consumer_group})
            tps = [TopicPartition(o.topic, o.partition, o.value)
                   for o in _offsets]
            logger.info(f'Set {len(tps)} offsets for consumer '
                        f'group: {consumer_group}')
            consumer.commit(offsets=tps, asynchronous=False)
