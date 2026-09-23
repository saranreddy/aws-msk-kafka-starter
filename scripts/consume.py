#!/usr/bin/env python3
"""
Kafka Consumer Script for AWS MSK Serverless

This script consumes messages from a Kafka topic on AWS MSK using IAM authentication.
"""

import argparse
import json
import logging
import signal
import sys

from kafka import KafkaConsumer
from kafka.errors import KafkaError

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Global flag for graceful shutdown
shutdown_flag = False


def signal_handler(signum, frame):
    """Handle shutdown signals gracefully."""
    global shutdown_flag
    logger.info("Shutdown signal received, closing consumer...")
    shutdown_flag = True


def create_consumer(
    bootstrap_servers: str, topic: str, group_id: str, region: str, from_beginning: bool
) -> KafkaConsumer:
    """
    Create a Kafka consumer with IAM authentication for MSK Serverless.

    Args:
        bootstrap_servers: Comma-separated list of bootstrap broker endpoints
        topic: Topic name to consume from
        group_id: Consumer group ID
        region: AWS region where MSK cluster is deployed
        from_beginning: Whether to consume from the beginning

    Returns:
        KafkaConsumer instance
    """
    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers.split(","),
            security_protocol="SASL_SSL",
            sasl_mechanism="AWS_MSK_IAM",
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            key_deserializer=lambda k: k.decode("utf-8") if k else None,
            group_id=group_id,
            auto_offset_reset="earliest" if from_beginning else "latest",
            enable_auto_commit=True,
            auto_commit_interval_ms=1000,
            client_id="msk-starter-consumer",
            region_name=region,
        )
        logger.info(f"Successfully connected to MSK cluster at {bootstrap_servers}")
        logger.info(f"Subscribed to topic: {topic}")
        logger.info(f"Consumer group: {group_id}")
        return consumer
    except Exception as e:
        logger.error(f"Failed to create consumer: {e}")
        raise


def consume_messages(consumer: KafkaConsumer, max_messages: int = None) -> None:
    """
    Consume messages from a Kafka topic.

    Args:
        consumer: KafkaConsumer instance
        max_messages: Maximum number of messages to consume (None for infinite)
    """
    message_count = 0

    try:
        logger.info("Starting to consume messages... (Press Ctrl+C to stop)")

        for message in consumer:
            if shutdown_flag:
                break

            message_count += 1

            logger.info(
                f"Received message {message_count} - "
                f"Topic: {message.topic}, "
                f"Partition: {message.partition}, "
                f"Offset: {message.offset}, "
                f"Key: {message.key}"
            )
            logger.info(f"Message value: {json.dumps(message.value, indent=2)}")

            if max_messages and message_count >= max_messages:
                logger.info(f"Reached max messages limit ({max_messages})")
                break

        logger.info(f"Total messages consumed: {message_count}")

    except KeyboardInterrupt:
        logger.info("Consumer interrupted by user")
    except KafkaError as e:
        logger.error(f"Kafka error: {e}")
        raise
    except Exception as e:
        logger.error(f"Error consuming messages: {e}")
        raise
    finally:
        consumer.close()
        logger.info("Consumer closed")


def main():
    parser = argparse.ArgumentParser(
        description="Consume messages from AWS MSK Kafka topic"
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
        "--group-id",
        default="msk-starter-consumer-group",
        help="Consumer group ID (default: msk-starter-consumer-group)",
    )
    parser.add_argument(
        "--from-beginning",
        action="store_true",
        help="Consume from the beginning of the topic",
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        default=None,
        help="Maximum number of messages to consume (default: infinite)",
    )
    parser.add_argument(
        "--region", default="us-east-1", help="AWS region (default: us-east-1)"
    )

    args = parser.parse_args()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    logger.info("Starting Kafka consumer...")
    logger.info(f"Bootstrap servers: {args.bootstrap_servers}")
    logger.info(f"Topic: {args.topic}")
    logger.info(f"Consumer group: {args.group_id}")
    logger.info(f"Region: {args.region}")
    logger.info(f"From beginning: {args.from_beginning}")

    try:
        consumer = create_consumer(
            args.bootstrap_servers,
            args.topic,
            args.group_id,
            args.region,
            args.from_beginning,
        )
        consume_messages(consumer, args.max_messages)
    except Exception as e:
        logger.error(f"Consumer failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
