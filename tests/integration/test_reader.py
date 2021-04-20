import pytest

from ksnap.reader import ConfluentKafkaReader


@pytest.fixture
def confluent_kafka_reader(KAFKA_HOSTS):
    return ConfluentKafkaReader(KAFKA_HOSTS)


def test_confluent_kafka_reader_read(confluent_kafka_reader: ConfluentKafkaReader):
    confluent_kafka_reader.subscribe(['CBBCAutomationInstrumentFitPOParameters'])
    topic_partition_message_dict = confluent_kafka_reader.read()
    assert topic_partition_message_dict
