from datetime import time
import pytest

from ksnap.reader import ConfluentKafkaReader, PythonKafkaReader


@pytest.fixture
def confluent_kafka_reader(KAFKA_HOSTS):
    return ConfluentKafkaReader(KAFKA_HOSTS)


@pytest.fixture
def python_kafka_reader(KAFKA_HOSTS):
    return PythonKafkaReader(KAFKA_HOSTS)



def test_confluent_kafka_reader_read(confluent_kafka_reader: ConfluentKafkaReader):
    confluent_kafka_reader.subscribe(['CBBCAutomationInstrumentFitPOParameters'])
    topic_partition_message_dict = confluent_kafka_reader.read(timeout=20)
    assert topic_partition_message_dict


def test_confluent_kafka_reader_read(python_kafka_reader: PythonKafkaReader):
    python_kafka_reader.subscribe(['CBBCAutomationInstrumentFitPOParameters'])
    topic_partition_message_dict = python_kafka_reader.read(timeout=20)
    assert topic_partition_message_dict
