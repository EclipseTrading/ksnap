"""Manager that glues everything together
"""
import logging
from multiprocessing.pool import ThreadPool

from ksnap.admin import ConfluentAdminClient
from ksnap.config import KsnapConfig
from ksnap.data_flow import DataFlowManager
from ksnap.offset import generate_new_offsets
from ksnap.partition import Partition
from ksnap.reader import ConfluentKafkaReader, PythonKafkaReader
from ksnap.writer import ConfluentKafkaWriter

logger = logging.getLogger(__name__)


class UnknownActionError(Exception):
    pass


class KsnapManager:

    def __init__(self, config: KsnapConfig):
        self.config = config

    def backup(self):
        # Read topic messages from kafka broker
        if self.config.kafka_library == 'confluent':
            reader = ConfluentKafkaReader(self.config.brokers)
        else:
            reader = PythonKafkaReader(self.config.brokers)
        if self.config.ignore_missing_topics:
            logger.debug('Filter out topics that are not in Kafka broker')
            broker_topic_names = reader.list_topics()
            topics = []
            for t in self.config.topics:
                if t not in broker_topic_names:
                    logger.debug(f'Ignore topic {t} since it is '
                                 'missing in kafka broker')
                    continue
                topics.append(t)
        else:
            topics = self.config.topics
        reader.subscribe(topics)
        msg_dict = reader.read(timeout=self.config.consumer_timeout)
        partitions = [
            Partition(topic, partition_no, msgs)
            for (topic, partition_no), msgs in msg_dict.items()
        ]
        # Fetch consumer group offsets
        admin_client = ConfluentAdminClient(self.config.brokers)
        offsets = admin_client.get_consumer_offsets(
            topics, no_of_threads=self.config.threads)
        # Write topic messages and consumer offsets to disk
        data_flow_manager = DataFlowManager(self.config.data)
        data_flow_manager.write(offsets, partitions)

    def restore(self):
        # Read topic messages and consumer offsets from disk
        data_flow_manager = DataFlowManager(self.config.data)
        offsets, partitions = data_flow_manager.read(self.config.topics)

        def func(partition):
            logger.debug(f'Write {len(partition.messages)} messages'
                         f'to topic: {partition.topic} '
                         f'partition: {partition.name} in kafka broker')
            writer = ConfluentKafkaWriter(self.config.brokers)

            i = 0
            for msg in partition.messages:
                i += 1
                writer.write(partition.topic, partition.name, msg)

                # TODO: Our queue seems to need flushing more often, try
                # every 10 messages first and see what happens.
                if i % 10 == 0:
                    writer.flush()

            writer.flush()
        # Write topic messages to kafka broker
        pool = ThreadPool(self.config.threads)
        pool.map(func, partitions)
        # Calculate new offsets
        new_offsets = generate_new_offsets(offsets, partitions)
        # Set consumer group offsets
        admin_client = ConfluentAdminClient(self.config.brokers)
        admin_client.set_consumer_offsets(new_offsets)

    def run(self):
        if self.config.action == 'backup':
            self.backup()
        elif self.config.action == 'restore':
            self.restore()
        else:
            raise UnknownActionError("Please use actions: [backup, restore]")
