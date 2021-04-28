from datetime import datetime
from time import sleep

import pytest
from confluent_kafka.admin import NewTopic
from ksnap.admin import ConfluentAdminClient
from ksnap.message import Message
from ksnap.writer import ConfluentKafkaWriter
from ksnap.reader  import ConfluentKafkaReader



@pytest.fixture
def confluent_kafka_writer():
    return ConfluentKafkaWriter(['localhost'])


def wait_for_futures(result_dict):
    return_dict = {}
    for key, val in result_dict.items():
        while not val.done():
            sleep(0.5)
        return_dict[key] = val.result()
    return return_dict


def test_confluent_kafka_writer_write(confluent_kafka_writer: ConfluentKafkaWriter):
    confluent_admin_client = ConfluentAdminClient(['localhost'])
    new_topic = NewTopic('Topic1', num_partitions=1, replication_factor=1)
    try:
        future_dict = confluent_admin_client.client.create_topics([new_topic])
        wait_for_futures(future_dict)
    except:
        pass
    confluent_kafka_writer.write('Topic1', 0, Message(1, 'Key1', 'Val1', int(datetime.now().timestamp())))
    sleep(5)
    confluent_kafka_reader = ConfluentKafkaReader(['localhost'])
    confluent_kafka_reader.subscribe(['Topic1'])
    partition_message_dict = confluent_kafka_reader.read(timeout=10)
    assert partition_message_dict
