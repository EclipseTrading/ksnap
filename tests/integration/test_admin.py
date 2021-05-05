import pytest
from ksnap.admin import ConfluentAdminClient


@pytest.fixture
def confluent_admin_client(KAFKA_HOSTS):
    return ConfluentAdminClient(KAFKA_HOSTS)


def test_get_consumer_groups(confluent_admin_client: ConfluentAdminClient):
    consumer_groups = confluent_admin_client.get_consumer_groups()
    assert consumer_groups


def test_get_consumer_offsets(confluent_admin_client: ConfluentAdminClient):
    offsets = confluent_admin_client.get_consumer_offsets(
        ['CBBCAutomationInstrumentFitPOParameters'], no_of_threads=8)
    assert offsets
