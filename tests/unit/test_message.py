from ksnap.message import Message


def test_message_to_row():
    msg = Message(100, 'key_1', 'val_1', 123456789, [('key_1', b'val_1'), ('key_2', None)])
    row = msg.to_row()
    assert len(row) == 5
    assert row == (100, 'key_1', 'val_1', 123456789, '[{"key": "key_1", "val": "dmFsXzE="}, {"key": "key_2", "val": null}]')

    msg = Message(100, None, 'val_1', 123456789, [('key_1', b'val_1'), ('key_2', None)])
    row = msg.to_row()
    assert len(row) == 5
    assert row == (100, None, 'val_1', 123456789, '[{"key": "key_1", "val": "dmFsXzE="}, {"key": "key_2", "val": null}]')


def test_message_from_row():
    msg = Message.from_row(100, 'key_1', 'val_1', 123456789, None)
    assert msg
    assert msg.offset == 100
    assert msg.key == 'key_1'
    assert msg.value == 'val_1'
    assert msg.timestamp == 123456789
    assert msg.headers is None

    msg = Message.from_row(100, None, 'val_1', 123456789, None)
    assert msg
    assert msg.offset == 100
    assert msg.key is None
    assert msg.value == 'val_1'
    assert msg.timestamp == 123456789
    assert msg.headers is None

    msg = Message.from_row(100, 'key_1', 'val_1', 123456789, '[{"key": "key_1", "val": "dmFsXzE="}, {"key": "key_2", "val": null}]')
    assert msg
    assert msg.offset == 100
    assert msg.key == 'key_1'
    assert msg.value == 'val_1'
    assert msg.timestamp == 123456789
    assert msg.headers == [('key_1', b'val_1'), ('key_2', None)]
