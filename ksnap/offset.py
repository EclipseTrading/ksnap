"""Deal with offsets and calculation of new offsets after restoration
"""
import sqlite3
from copy import copy
from dataclasses import dataclass
from typing import List

from ksnap.partition import Partition


class OffsetFileReadError(Exception):
    pass


@dataclass
class Offset:
    consumer_group: str
    topic: str
    partition: int
    value: int


class OffsetManager:
    """TODO: Should rename this to something a bit more
    """
    def __init__(self, offsets: List[Offset]):
        self.offsets = offsets

    def to_file(self, db_file_path: str):
        conn = sqlite3.connect(db_file_path)
        cursor = conn.cursor()
        create_table_query = (
            "CREATE TABLE IF NOT EXISTS offset "
            "(consumer_group TEXT, "
            "topic TEXT NOT NULL, "
            "partition INTEGER, "
            "value INTEGER)"
        )
        cursor.execute(create_table_query)
        conn.commit()
        cursor.executemany("INSERT INTO offset VALUES(?,?,?,?);",
                           [(o.consumer_group, o.topic, o.partition, o.value)
                            for o in self.offsets])
        conn.commit()
        conn.close()

    @classmethod
    def from_file(cls, db_file_path: str):
        try:
            conn = sqlite3.connect(db_file_path)
            cursor = conn.cursor()
            # read messages
            offset_query = "SELECT * FROM offset"
            cursor.execute(offset_query)
            rows = cursor.fetchall()
        except Exception as exc:
            raise OffsetFileReadError from exc
        finally:
            conn.close()
        return cls([Offset(*r) for r in rows])


def _generate_partition_dict(partitions: List[Partition]):
    return {(p.topic, p.name): p for p in partitions}


def _calculate_new_offset(curr_offset_val: int, partition: Partition) -> int:
    # current offset points to the read message offset + 1
    last_read_offset = curr_offset_val - 1
    # start count from 1 so we don't need to shift offset by 1 when returning
    for index, msg in enumerate(partition.messages, 1):
        if msg.offset == last_read_offset:
            return index
        if msg.offset > last_read_offset:
            return index - 1
    return index


def generate_new_offsets(offsets: List[Offset],
                         partitions: List[Partition]) -> List[Offset]:
    partition_dict = _generate_partition_dict(partitions)
    new_offsets = []
    for offset in offsets:
        # TODO: Add exception handling
        key = (offset.topic, offset.partition)
        if key not in partition_dict:
            continue
        partition = partition_dict[key]
        new_offset_value = _calculate_new_offset(
            offset.value, partition)
        new_offset = copy(offset)
        new_offset.value = new_offset_value
        new_offsets.append(new_offset)
    return new_offsets
