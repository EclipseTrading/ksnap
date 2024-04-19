import logging
from abc import ABC, abstractmethod
from datetime import datetime, timedelta

from typing import List, Set

logger = logging.getLogger(__name__)


class KafkaReader(ABC):

    @staticmethod
    def _check_timeout(timeout: int, start_time: datetime) -> bool:
        if not timeout:
            return False
        timeout_td = timedelta(seconds=timeout)
        current_td = datetime.now() - start_time
        return current_td > timeout_td

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

    @staticmethod
    @abstractmethod
    def _check_reach_offsets(msg, offset_dict):
        pass

    @abstractmethod
    def subscribe(self, topics: List[str]):
        pass

    @abstractmethod
    def read(self, timeout: int = 0):
        pass

    @abstractmethod
    def list_topics(self) -> Set[str]:
        pass

    @abstractmethod
    def close(self):
        pass
