#!/usr/bin/env python3
"""
Smoke Test Script for AWS MSK Kafka Starter

This script performs an end-to-end smoke test by:
1. Creating/ensuring the Kafka topic exists
2. Producing a fixed set of test messages
3. Consuming the messages and validating they were received
4. Exiting with status 0 only if all messages were successfully produced and consumed

Requires infrastructure to be deployed and network connectivity to MSK brokers.
"""

import argparse
import json
import logging
import sys
import time
from datetime import datetime
from typing import List, Set

from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import KafkaError, TopicAlreadyExistsError

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class SmokeTest:
    """Orchestrates end-to-end smoke test for MSK Kafka."""

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        region: str,
        message_count: int = 5,
        timeout: int = 60,
    ):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.region = region
        self.message_count = message_count
        self.timeout = timeout
        self.test_id = datetime.utcnow().strftime("%Y%m%d%H%M%S")

    def create_admin_client(self) -> KafkaAdminClient:
        """Create Kafka admin client with IAM auth."""
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=self.bootstrap_servers.split(","),
                security_protocol="SASL_SSL",
                sasl_mechanism="AWS_MSK_IAM",
                client_id="msk-starter-smoke-admin",
                region_name=self.region,
            )
            logger.info(f"✅ Connected to MSK cluster at {self.bootstrap_servers}")
            return admin_client
        except Exception as e:
            logger.error(f"❌ Failed to create admin client: {e}")
            raise

    def ensure_topic_exists(self) -> bool:
        """Ensure the topic exists, create if it doesn't."""
        logger.info(f"Checking if topic '{self.topic}' exists...")

        admin_client = None
        try:
            admin_client = self.create_admin_client()

            # Check if topic exists
            topics = admin_client.list_topics()
            if self.topic in topics:
                logger.info(f"✅ Topic '{self.topic}' already exists")
                return True

            # Create topic
            logger.info(f"Creating topic '{self.topic}'...")
            new_topic = NewTopic(
                name=self.topic, num_partitions=2, replication_factor=2
            )

            result = admin_client.create_topics([new_topic], validate_only=False)

            for topic_name, future in result.items():
                try:
                    future.result()
                    logger.info(f"✅ Created topic '{topic_name}'")
                    return True
                except TopicAlreadyExistsError:
                    logger.info(f"✅ Topic '{topic_name}' already exists")
                    return True
                except KafkaError as e:
                    logger.error(f"❌ Failed to create topic '{topic_name}': {e}")
                    return False

        except Exception as e:
            logger.error(f"❌ Error ensuring topic exists: {e}")
            return False
        finally:
            if admin_client:
                admin_client.close()

    def create_producer(self) -> KafkaProducer:
        """Create Kafka producer with IAM auth."""
        try:
            producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers.split(","),
                security_protocol="SASL_SSL",
                sasl_mechanism="AWS_MSK_IAM",
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                client_id="msk-starter-smoke-producer",
                acks="all",
                retries=3,
                max_in_flight_requests_per_connection=1,
                region_name=self.region,
            )
            return producer
        except Exception as e:
            logger.error(f"❌ Failed to create producer: {e}")
            raise

    def produce_test_messages(self) -> List[str]:
        """Produce test messages and return list of message IDs."""
        logger.info(f"Producing {self.message_count} test messages...")

        producer = None
        message_ids = []

        try:
            producer = self.create_producer()

            for i in range(self.message_count):
                message_id = f"smoke-test-{self.test_id}-{i + 1}"
                message = {
                    "test_id": self.test_id,
                    "message_id": message_id,
                    "sequence": i + 1,
                    "timestamp": datetime.utcnow().isoformat(),
                    "data": f"Smoke test message {i + 1}/{self.message_count}",
                }

                key = f"smoke-test-{i + 1}"

                future = producer.send(self.topic, key=key, value=message)
                record_metadata = future.get(timeout=10)

                message_ids.append(message_id)
                logger.info(
                    f"  ✅ Sent message {i + 1}/{self.message_count} "
                    f"(partition: {record_metadata.partition}, "
                    f"offset: {record_metadata.offset})"
                )

            producer.flush()
            logger.info(f"✅ Successfully produced {len(message_ids)} messages")
            return message_ids

        except Exception as e:
            logger.error(f"❌ Failed to produce messages: {e}")
            raise
        finally:
            if producer:
                producer.close()

    def create_consumer(self) -> KafkaConsumer:
        """Create Kafka consumer with IAM auth."""
        try:
            consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=self.bootstrap_servers.split(","),
                security_protocol="SASL_SSL",
                sasl_mechanism="AWS_MSK_IAM",
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                key_deserializer=lambda k: k.decode("utf-8") if k else None,
                group_id=f"smoke-test-consumer-{self.test_id}",
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                client_id="msk-starter-smoke-consumer",
                consumer_timeout_ms=self.timeout * 1000,
                region_name=self.region,
            )
            return consumer
        except Exception as e:
            logger.error(f"❌ Failed to create consumer: {e}")
            raise

    def consume_and_verify(self, expected_message_ids: List[str]) -> bool:
        """Consume messages and verify all expected messages were received."""
        logger.info(
            f"Consuming messages (timeout: {self.timeout}s, "
            f"expecting {len(expected_message_ids)} messages from this test)..."
        )

        consumer = None
        received_message_ids: Set[str] = set()
        expected_ids_set = set(expected_message_ids)

        try:
            consumer = self.create_consumer()

            for message in consumer:
                try:
                    message_id = message.value.get("message_id")
                    test_id = message.value.get("test_id")

                    # Only count messages from this smoke test run
                    if test_id == self.test_id and message_id in expected_ids_set:
                        received_message_ids.add(message_id)
                        sequence = message.value.get("sequence", "?")
                        logger.info(
                            f"  ✅ Received message {sequence}/{self.message_count} "
                            f"(partition: {message.partition}, offset: {message.offset})"
                        )

                        # Check if we've received all expected messages
                        if received_message_ids == expected_ids_set:
                            logger.info("✅ All expected messages received")
                            break

                except (KeyError, AttributeError, json.JSONDecodeError) as e:
                    logger.warning(f"⚠️  Skipping malformed message: {e}")
                    continue

            # Verify all messages were received
            missing = expected_ids_set - received_message_ids
            if missing:
                logger.error(
                    f"❌ Missing {len(missing)} messages: {sorted(missing)[:5]}..."
                )
                return False

            logger.info(
                f"✅ Successfully consumed {len(received_message_ids)} messages"
            )
            return True

        except Exception as e:
            logger.error(f"❌ Failed to consume messages: {e}")
            return False
        finally:
            if consumer:
                consumer.close()

    def run(self) -> bool:
        """Run the complete smoke test."""
        logger.info("=" * 80)
        logger.info("AWS MSK Kafka Smoke Test")
        logger.info(f"Test ID: {self.test_id}")
        logger.info(f"Bootstrap: {self.bootstrap_servers}")
        logger.info(f"Topic: {self.topic}")
        logger.info(f"Region: {self.region}")
        logger.info("=" * 80 + "\n")

        try:
            # Step 1: Ensure topic exists
            logger.info("Step 1: Ensuring topic exists...")
            if not self.ensure_topic_exists():
                logger.error("❌ Failed to ensure topic exists")
                return False

            # Wait a moment for topic to be ready
            time.sleep(2)

            # Step 2: Produce test messages
            logger.info("\nStep 2: Producing test messages...")
            message_ids = self.produce_test_messages()

            if not message_ids:
                logger.error("❌ No messages were produced")
                return False

            # Wait a moment for messages to be committed
            time.sleep(2)

            # Step 3: Consume and verify messages
            logger.info("\nStep 3: Consuming and verifying messages...")
            verified = self.consume_and_verify(message_ids)

            if not verified:
                logger.error("❌ Message verification failed")
                return False

            # All steps passed
            logger.info("\n" + "=" * 80)
            logger.info("✅ SMOKE TEST PASSED")
            logger.info("All messages were successfully produced and consumed")
            logger.info("=" * 80)
            return True

        except Exception as e:
            logger.error(f"\n❌ SMOKE TEST FAILED: {e}")
            return False


def main():
    parser = argparse.ArgumentParser(
        description="End-to-end smoke test for AWS MSK Kafka"
    )
    parser.add_argument(
        "--bootstrap-servers",
        required=True,
        help="Comma-separated list of bootstrap broker endpoints",
    )
    parser.add_argument(
        "--topic", default="demo-topic", help="Kafka topic name (default: demo-topic)"
    )
    parser.add_argument(
        "--region", default="us-east-1", help="AWS region (default: us-east-1)"
    )
    parser.add_argument(
        "--message-count",
        type=int,
        default=5,
        help="Number of test messages (default: 5)",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=60,
        help="Consumer timeout in seconds (default: 60)",
    )

    args = parser.parse_args()

    smoke_test = SmokeTest(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        region=args.region,
        message_count=args.message_count,
        timeout=args.timeout,
    )

    try:
        success = smoke_test.run()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        logger.error("\n❌ Smoke test interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"\n❌ Smoke test failed with error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
