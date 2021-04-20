import pytest


@pytest.fixture
def KAFKA_HOSTS():
    return ['hkstgkafka02.hk.eclipseoptions.com',
            'hkstgkafka03.hk.eclipseoptions.com',
            'hkstgkafka04.hk.eclipseoptions.com',
            'hkstgkafka05.hk.eclipseoptions.com',
            'hkstgkafka06.hk.eclipseoptions.com']
