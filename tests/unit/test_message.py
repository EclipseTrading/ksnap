from ksnap.message import Message


def test_message_to_row():
    msg = Message(100, 'key_1', 'val_1', 123456789, [('key_1', b'val_1')])
    row = msg.to_row()
    assert len(row) == 5
    assert row == (100, 'key_1', 'val_1', 123456789, '[{"key": "key_1", "val": "dmFsXzE="}]')


def test_message_from_row():
    msg = Message.from_row(100, 'key_1', 'val_1', 123456789, None)
    assert msg
    assert msg.offset == 100
    assert msg.key == 'key_1'
    assert msg.value == 'val_1'
    assert msg.timestamp == 123456789
    assert msg.headers is None
