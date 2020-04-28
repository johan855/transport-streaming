import time
import logging

from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

logger = logging.getLogger(__name__)


BROKER_URL = "PLAINTEXT://kafka0:9092,PLAINTEXT://kafka1:9093,PLAINTEXT://kafka2:9094",
SCHEMA_REGISTRY_URL = "http://schema-registry:8081"


class Producer:
    """Defines and provides common functionality amongst Producers"""

    #Track existing topics accross all Producer instances
    existing_topics = set([])

    def __init__(
            self,
            topic_name,
            key_schema,
            value_schema=None,
            num_partitions=1,
            num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        """Initializes Broker settings"""
        self.broker_properties = {
            "bootstrap.servers": BROKER_URL,
            "schema.registry.url": SCHEMA_REGISTRY_URL
        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        """Initialize Schema setting"""
        self.producer = AvroProducer(
            self.broker_properties,
            default_key_schema=self.key_schema,
            default_value_schema=self.value_schema
        )

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        """Check missing config on client.create_topics()"""
        client = AdminClient(
            {"bootstrap.servers": self.broker_properties["bootstrap.servers"]}
        )
        topics_list = client.list_topics(timeout=3)
        if self.topic_name in set(t.topic for t in iter(topics_list.topics.values())):
            logger.info("topic already exists, skipping...")
            return
        topic_name = self.topic_name
        logger.info(f"creating topic {topic_name}")
        futures = client.create_topics(
            [NewTopic(
                topic = topic_name,
                num_partitions = self.num_partitions,
                replication_factor = self.num_replicas
            )]
        )


    def time_millis(self):
        return int(round(time.time() * 1000))

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        if self.topic_name != None:
            self.producer.flush()
            logger.info("producer flushed")
        else:
            logger.info("producer close incomplete - skipping")