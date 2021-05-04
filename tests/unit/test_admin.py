from ksnap.admin import ConfluentAdminClient
import pytest
import confluent_kafka
import confluent_kafka.admin

class MockedConsumer:

    def __init__(self, *args, **kwargs):
        pass

    def committed(self, partitions, timeout=None):
        topic = 'Topic1'
        partition = 1
        value = 100
        tp = confluent_kafka.TopicPartition(topic, partition, value)
        return [tp]

    def close(self):
        pass


class MockedAdmin:

    def __init__(self, *args, **kwargs):
        pass

    def list_topics(self):
        class T:
            def __init__(self, p):
                self.topics = {'Topic1': p}
        class P:
            def __init__(self, p):
                self.partitions = p

        p = P([1])
        t = T(p)
        return t

    def list_groups(self):
        class G:
            def __init__(self, id_):
                self.id = id_
        return [G('CG1')]

    def close(self):
        pass


def test_confluent_admin_client_get_consumer_offsets(monkeypatch):
    monkeypatch.setattr(confluent_kafka, 'Consumer', MockedConsumer)
    confluent_admin_client = ConfluentAdminClient(['foo_baz_1'])
    confluent_admin_client.client = MockedAdmin()
    offsets = confluent_admin_client.get_consumer_offsets(['Topic1'])
    assert offsets
    offsets = confluent_admin_client.get_consumer_offsets(['Topic1'],
                                                          no_of_threads=2)
    assert offsets
