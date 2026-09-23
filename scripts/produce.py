#!/usr/bin/env python3
"""
Kafka Producer Script for AWS MSK Serverless

This script produces messages to a Kafka topic on AWS MSK using IAM authentication.
"""

import argparse
import json
import logging
import sys
import time
from datetime import datetime

from kafka import KafkaProducer
from kafka.errors import KafkaError

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def create_producer(bootstrap_servers: str, region: str) -> KafkaProducer:
    """
    Create a Kafka producer with IAM authentication for MSK Serverless.
    
    Args:
        bootstrap_servers: Comma-separated list of bootstrap broker endpoints
        region: AWS region where MSK cluster is deployed
        
    Returns:
        KafkaProducer instance
    """
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers.split(','),
            security_protocol='SASL_SSL',
            sasl_mechanism='AWS_MSK_IAM',
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            client_id='msk-starter-producer',
            acks='all',
            retries=3,
            max_in_flight_requests_per_connection=1,
            region_name=region
        )
        logger.info(f"Successfully connected to MSK cluster at {bootstrap_servers}")
        return producer
    except Exception as e:
        logger.error(f"Failed to create producer: {e}")
        raise


def produce_messages(
    producer: KafkaProducer,
    topic: str,
    count: int,
    interval: float
) -> None:
    """
    Produce messages to a Kafka topic.
    
    Args:
        producer: KafkaProducer instance
        topic: Topic name to produce to
        count: Number of messages to produce
        interval: Interval between messages in seconds
    """
    try:
        for i in range(count):
            message = {
                'message_id': i + 1,
                'timestamp': datetime.utcnow().isoformat(),
                'data': f'Hello from AWS MSK Kafka Starter - Message {i + 1}'
            }
            
            key = f"key-{i + 1}"
            
            future = producer.send(topic, key=key, value=message)
            
            try:
                record_metadata = future.get(timeout=10)
                logger.info(
                    f"Sent message {i + 1}/{count} - "
                    f"Topic: {record_metadata.topic}, "
                    f"Partition: {record_metadata.partition}, "
                    f"Offset: {record_metadata.offset}"
                )
            except KafkaError as e:
                logger.error(f"Failed to send message {i + 1}: {e}")
                continue
            
            if i < count - 1:
                time.sleep(interval)
        
        producer.flush()
        logger.info(f"Successfully produced {count} messages to topic '{topic}'")
        
    except KeyboardInterrupt:
        logger.info("Producer interrupted by user")
    except Exception as e:
        logger.error(f"Error producing messages: {e}")
        raise
    finally:
        producer.close()
        logger.info("Producer closed")


def main():
    parser = argparse.ArgumentParser(
        description='Produce messages to AWS MSK Kafka topic'
    )
    parser.add_argument(
        '--bootstrap-servers',
        required=True,
        help='Comma-separated list of bootstrap broker endpoints'
    )
    parser.add_argument(
        '--topic',
        default='demo-topic',
        help='Kafka topic name (default: demo-topic)'
    )
    parser.add_argument(
        '--count',
        type=int,
        default=10,
        help='Number of messages to produce (default: 10)'
    )
    parser.add_argument(
        '--interval',
        type=float,
        default=1.0,
        help='Interval between messages in seconds (default: 1.0)'
    )
    parser.add_argument(
        '--region',
        default='us-east-1',
        help='AWS region (default: us-east-1)'
    )
    
    args = parser.parse_args()
    
    logger.info("Starting Kafka producer...")
    logger.info(f"Bootstrap servers: {args.bootstrap_servers}")
    logger.info(f"Topic: {args.topic}")
    logger.info(f"Messages to produce: {args.count}")
    logger.info(f"Region: {args.region}")
    
    try:
        producer = create_producer(args.bootstrap_servers, args.region)
        produce_messages(producer, args.topic, args.count, args.interval)
    except Exception as e:
        logger.error(f"Producer failed: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
