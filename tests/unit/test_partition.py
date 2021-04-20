from ksnap.message import Message
import os
import sqlite3

from ksnap.partition import Partition


def test_partition():
    assert Partition('topic_name', 1, [])


def test_partition_to_file(tmpdir):
    partition = Partition(
        'topic_name_1', 1, [Message(1, 'Key', 'Message_1', 123456789, [('key', b'val_1')])])
    file_path = os.path.join(tmpdir, 'topic_name_1_1.sqlite3')
    partition.to_file(file_path)
    conn = sqlite3.connect(file_path)
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM metadata LIMIT 1')
    metadata = cursor.fetchone()
    assert metadata == ('topic_name_1', 1)
    cursor.execute('SELECT * FROM data')
    messages = cursor.fetchmany()
    msgs = [Message.from_row(*m) for m in messages]
    assert msgs
    assert len(msgs) == 1
    assert msgs == [Message(1, 'Key', 'Message_1', 123456789, [('key', b'val_1')])]
    conn.close()


def test_partition_from_file(tmpdir):
    partition = Partition(
        'topic_name_1', 1, [Message(1, 'Key', 'Message_1', 123456789)])
    file_path = os.path.join(tmpdir, 'topic_name_1_1.sqlite3')
    partition.to_file(file_path)
    from_file_partition = Partition.from_file(file_path)
    assert from_file_partition.topic == partition.topic
    assert from_file_partition.name == partition.name
    assert from_file_partition.messages == partition.messages
