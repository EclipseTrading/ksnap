import os

from ksnap.data_flow import DataFlowManager
from ksnap.message import Message
from ksnap.offset import Offset
from ksnap.partition import Partition


def test_data_flow_manager_write(tmpdir):
    data_flow_manager = DataFlowManager(tmpdir)
    offsets = [Offset('group_1', 'topic_1', 0, 10),
               Offset('group_1', 'topic_2', 0, 1)]
    partitions = [Partition('topic_1', 0, [Message(0, 'key_1', 'val_1',
                                           123456789, None)]),
                  Partition('topic_1', 1, [Message(0, 'key_1', 'val_1',
                                           123456789, None)]),
                  Partition('topic_2', 0, [Message(0, 'key_1', 'val_1',
                                           123456789, None)]),
                 ]
    data_flow_manager.write(offsets, partitions)
    assert os.path.isdir(os.path.join(tmpdir, 'partitions'))
    assert len(os.listdir(os.path.join(tmpdir, 'partitions'))) == 3
    assert os.path.isfile(os.path.join(tmpdir, 'offsets.sqlite3'))


def test_data_flow_manager_read(tmpdir):
    data_flow_manager = DataFlowManager(tmpdir)
    offsets = [Offset('group_1', 'topic_1', 0, 10),
               Offset('group_1', 'topic_2', 0, 1)]
    partitions = [Partition('topic_1', 0, [Message(0, 'key_1', 'val_1',
                                           123456789, None)]),
                  Partition('topic_1', 1, [Message(0, 'key_1', 'val_1',
                                           123456789, None)]),
                  Partition('topic_2', 0, [Message(0, 'key_1', 'val_1',
                                           123456789, None)]),
                 ]
    data_flow_manager.write(offsets, partitions)
    from_disk_offsets, from_disk_partitions = data_flow_manager.read()
    from_disk_partitions.sort(key=lambda p: (p.topic, p.name))
    partitions.sort(key=lambda p: (p.topic, p.name))
    assert from_disk_offsets == offsets
    assert from_disk_partitions == partitions
