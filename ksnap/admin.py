"""Kafka admin client for consumer group related operations.
"""
import logging
import re
from abc import abstractmethod
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import DefaultDict, Dict, List

import confluent_kafka as ck
from confluent_kafka.admin import AdminClient

from ksnap.offset import Offset

IGNORE_GROUP_REGEX = '[a-f0-9]{8}(-[a-f0-9]{4}){4}[a-f0-9]{8}'
CONSUMER_OFFSET_TIMEOUT = 60
logger = logging.getLogger(__name__)


class ConsumerOffsetError(Exception):
    pass


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

    @property
    def consumer_config(self):
        return {
            **self.config,
            "max.poll.interval.ms": 10000,
            "auto.offset.reset": "smallest",
            "enable.auto.commit": False,
        }

    @staticmethod
    def _group_offsets(
            offsets: List[Offset]) -> Dict[str, List[Offset]]:
        d: DefaultDict[str, List[Offset]] = defaultdict(list)
        for offset in offsets:
            d[offset.consumer_group].append(offset)
        return d

    @staticmethod
    def _get_offsets(consumer_group, partitions, config,
                     timeout=CONSUMER_OFFSET_TIMEOUT) -> List[Offset]:
        offsets = []
        consumer = ck.Consumer({**config, 'group.id': consumer_group})
        for tp in consumer.committed(partitions, timeout=timeout):
            if tp.offset == -1001:
                continue
            offset = Offset(consumer_group, tp.topic,
                            tp.partition, tp.offset)
            offsets.append(offset)
        consumer.close()
        return offsets

    def _threaded_get_offsets(self, partitions, consumer_groups,
                              no_of_threads) -> List[Offset]:
        offsets: List[Offset] = []
        with ThreadPoolExecutor(max_workers=no_of_threads) as executor:
            futures = {executor.submit(ConfluentAdminClient._get_offsets,
                                       cg, partitions, self.config): cg
                       for cg in consumer_groups}
            for future in as_completed(futures):
                cg = futures[future]
                try:
                    _offsets = future.result()
                except Exception as exc:
                    msg = f'Encountered error when reading comsumer offset ' \
                          f'for consumer group: {cg}'
                    raise ConsumerOffsetError(msg) from exc
                offsets.extend(_offsets)
        return offsets

    def get_consumer_groups(self) -> List[str]:
        consumer_groups = self.client.list_groups()
        return [g.id for g in consumer_groups]

    def get_consumer_offsets(
        self, topics: List[str], ignore_group_regex: str = IGNORE_GROUP_REGEX,
        no_of_threads: int = 1
            ) -> List[Offset]:
        broker_topics = self.client.list_topics().topics
        partitions = []
        for topic_name in topics:
            partitions.extend([ck.TopicPartition(topic_name, k)
                               for k in broker_topics[topic_name].partitions])
        consumer_groups = []
        logger.info('Fetch consumer groups from broker')
        for consumer_group in self.get_consumer_groups():
            if re.findall(ignore_group_regex, consumer_group):
                logger.debug(f'Ignoring consumer group: {consumer_group}')
                continue
            consumer_groups.append(consumer_group)
        logger.info(f'Fetch consumer offsets for {len(consumer_groups)} '
                    'consumer groups')
        if no_of_threads == 1:
            offsets: List[Offset] = []
            for cg in consumer_groups:
                _offsets = ConfluentAdminClient._get_offsets(
                    cg, partitions, self.consumer_config,)
                offsets.extend(_offsets)
            return offsets
        return self._threaded_get_offsets(partitions, consumer_groups,
                                          no_of_threads)

    def set_consumer_offsets(self, offsets: List[Offset]):
        grouped_offsets = ConfluentAdminClient._group_offsets(
            offsets)
        for consumer_group, _offsets in grouped_offsets.items():
            consumer = ck.Consumer({**self.consumer_config,
                                    'group.id': consumer_group})
            tps = [ck.TopicPartition(o.topic, o.partition, o.value)
                   for o in _offsets]
            logger.info(f'Set {len(tps)} offsets for consumer '
                        f'group: {consumer_group}')
            consumer.commit(offsets=tps, asynchronous=False)
