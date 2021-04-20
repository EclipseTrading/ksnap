import os
import sqlite3
import pytest

from ksnap.message import Message
from ksnap.partition import Partition
from ksnap.offset import (Offset, OffsetManager, _calculate_new_offset,
                          _generate_partition_dict, generate_new_offsets)

PARTITIONS = Partition(
    'topic_1', 1, [Message(0, 'key_1', 'val_1', 123456789, None),
                   Message(99, 'key_2', 'val_2', 123456789, None),
                   Message(100, 'key_3', 'val_3', 123456789, None),
                   Message(200, 'key_4', 'val_4', 123456789, None),
                   Message(499, 'key_5', 'val_5', 123456789, None),])


@pytest.mark.parametrize(
    'curr_offset_val, partition, expected',
    [
        (100, PARTITIONS, 2),
        (1, PARTITIONS, 1),
        (500, PARTITIONS, 5),
        (600, PARTITIONS, 5),
    ]
)
def test_calculate_new_offset(curr_offset_val, partition, expected):
    new_offset = _calculate_new_offset(curr_offset_val, partition)
    assert new_offset == expected


def test_generate_new_offsets():
    offsets = [Offset('group_1', 'topic_1', 0, 100),
               Offset('group_1', 'topic_1', 1, 200),
               Offset('group_1', 'topic_1', 2, 500),
               Offset('group_1', 'topic_1', 3, 505)]
    partitions = [
        Partition('topic_1', 0, [Message(1, 'key_1', 'val_1', 123456789, None),
                                 Message(99, 'key_2', 'val_2', 123456789, None),
                                 Message(101, 'key_3', 'val_3', 123456789, None),
                                 Message(199, 'key_4', 'val_4', 123456789, None),
                                 Message(499, 'key_5', 'val_5', 123456789, None),]),
        Partition('topic_1', 1, [Message(1, 'key_1', 'val_1', 123456789, None),
                                 Message(99, 'key_2', 'val_2', 123456789, None),
                                 Message(101, 'key_3', 'val_3', 123456789, None),
                                 Message(199, 'key_4', 'val_4', 123456789, None),
                                 Message(499, 'key_5', 'val_5', 123456789, None),]),
        Partition('topic_1', 2, [Message(1, 'key_1', 'val_1', 123456789, None),
                                 Message(99, 'key_2', 'val_2', 123456789, None),
                                 Message(101, 'key_3', 'val_3', 123456789, None),
                                 Message(199, 'key_4', 'val_4', 123456789, None),
                                 Message(499, 'key_5', 'val_5', 123456789, None),]),
        Partition('topic_1', 3, [Message(1, 'key_1', 'val_1', 123456789, None),
                                 Message(99, 'key_2', 'val_2', 123456789, None),
                                 Message(101, 'key_3', 'val_3', 123456789, None),
                                 Message(199, 'key_4', 'val_4', 123456789, None),
                                 Message(499, 'key_5', 'val_5', 123456789, None),])
    ]
    new_offsets = generate_new_offsets(offsets, partitions)
    assert new_offsets == [Offset(consumer_group='group_1', topic='topic_1', partition=0, value=2),
                           Offset(consumer_group='group_1', topic='topic_1', partition=1, value=4),
                           Offset(consumer_group='group_1', topic='topic_1', partition=2, value=5),
                           Offset(consumer_group='group_1', topic='topic_1', partition=3, value=5)]


def test_offset_manager_to_file(tmpdir):
    offsets = [Offset(consumer_group='group_1', topic='topic_1', partition=0, value=1),
               Offset(consumer_group='group_1', topic='topic_1', partition=1, value=3),
               Offset(consumer_group='group_1', topic='topic_1', partition=2, value=4)]
    offset_manager = OffsetManager(offsets)
    file_path = os.path.join(tmpdir, 'offset.sqlite3')
    offset_manager.to_file(file_path)
    conn = sqlite3.connect(file_path)
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM offset')
    rows = cursor.fetchall()
    assert rows
    assert len(rows) == 3
    assert [Offset(*r) for r in rows] == offsets


def test_offset_manager_from_file(tmpdir):
    offsets = [Offset(consumer_group='group_1', topic='topic_1', partition=0, value=1),
               Offset(consumer_group='group_1', topic='topic_1', partition=1, value=3),
               Offset(consumer_group='group_1', topic='topic_1', partition=2, value=4)]
    offset_manager = OffsetManager(offsets)
    file_path = os.path.join(tmpdir, 'offset.sqlite3')
    offset_manager.to_file(file_path)
    from_file_offset_manager = OffsetManager.from_file(file_path)
    assert from_file_offset_manager.offsets == offset_manager.offsets
