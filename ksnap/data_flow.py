"""Back up and restoration functionalities that dictates how the data is
written and read.
"""
from ksnap.offset import Offset, OffsetManager
import os
import logging
from typing import List, Optional, Tuple

from ksnap.partition import Partition

logger = logging.getLogger(__name__)


class DataFlowManager:
    def __init__(self, data_dir: str):
        self.data_dir = data_dir

    @property
    def offset_file_path(self):
        return os.path.join(self.data_dir, 'offsets.sqlite3')

    @property
    def partition_file_dir(self):
        return os.path.join(self.data_dir, 'partitions')

    def read(self, topics: Optional[List[str]] = None
             ) -> Tuple[List[Offset], List[Partition]]:
        partitions = []
        for file_name in os.listdir(self.partition_file_dir):
            if not file_name.endswith('sqlite3'):
                continue
            file_path = os.path.join(self.partition_file_dir, file_name)
            partition = Partition.from_file(file_path)
            logger.debug(f'Read {len(partition.messages)} messages for topic: '
                         f'{partition.topic} partition: {partition.name} '
                         'from disk')
            # TODO: Consider using filename as filter or Write required
            # metadata about filename and topic/partition in one main sqlite3
            # file.
            if topics and partition.topic not in topics:
                continue
            partitions.append(partition)
        offset_manager = OffsetManager.from_file(self.offset_file_path)
        if topics:
            offsets = [o for o in offset_manager.offsets if o.topic in topics]
        offsets = offset_manager.offsets
        return (offsets, partitions)

    def write_partitions(self, partitions: List[Partition]):
        os.makedirs(self.partition_file_dir, exist_ok=True)
        logger.info(f'Write {len(partitions)} partitions '
                    f'to {self.partition_file_dir}')
        for partition in partitions:
            file_path = os.path.join(
                self.partition_file_dir,
                f'{partition.topic}_{partition.name}.sqlite3')
            logger.debug(
                f'Write {len(partition.messages)} messages '
                f'from topic: {partition.topic} '
                f'partition: {partition.name} to disk')
            partition.to_file(file_path)

    def write_offsets(self, offsets: List[Offset]):
        os.makedirs(self.partition_file_dir, exist_ok=True)
        logger.info(f'Write {len(offsets)} consumer group offsets to disk')
        OffsetManager(offsets).to_file(self.offset_file_path)

    def write(self, offsets: List[Offset], partitions: List[Partition]):
        self.write_partitions(partitions)
        self.write_offsets(offsets)
