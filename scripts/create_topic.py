#!/usr/bin/env python3
"""
Kafka Topic Creation Script for AWS MSK Serverless

This script creates a Kafka topic on AWS MSK using IAM authentication.
"""

import argparse
import logging
import sys

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, KafkaError

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def create_admin_client(bootstrap_servers: str, region: str) -> KafkaAdminClient:
    """
    Create a Kafka admin client with IAM authentication.
    
    Args:
        bootstrap_servers: Comma-separated list of bootstrap broker endpoints
        region: AWS region where MSK cluster is deployed
        
    Returns:
        KafkaAdminClient instance
    """
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers.split(','),
            security_protocol='SASL_SSL',
            sasl_mechanism='AWS_MSK_IAM',
            client_id='msk-starter-admin',
            region_name=region
        )
        logger.info(f"Successfully connected to MSK cluster at {bootstrap_servers}")
        return admin_client
    except Exception as e:
        logger.error(f"Failed to create admin client: {e}")
        raise


def create_topic(
    admin_client: KafkaAdminClient,
    topic_name: str,
    num_partitions: int,
    replication_factor: int
) -> None:
    """
    Create a Kafka topic.
    
    Args:
        admin_client: KafkaAdminClient instance
        topic_name: Name of the topic to create
        num_partitions: Number of partitions
        replication_factor: Replication factor
    """
    try:
        topic = NewTopic(
            name=topic_name,
            num_partitions=num_partitions,
            replication_factor=replication_factor
        )
        
        result = admin_client.create_topics([topic], validate_only=False)
        
        for topic, future in result.items():
            try:
                future.result()
                logger.info(f"Successfully created topic '{topic}'")
            except TopicAlreadyExistsError:
                logger.warning(f"Topic '{topic}' already exists")
            except KafkaError as e:
                logger.error(f"Failed to create topic '{topic}': {e}")
                raise
        
    except Exception as e:
        logger.error(f"Error creating topic: {e}")
        raise
    finally:
        admin_client.close()
        logger.info("Admin client closed")


def list_topics(admin_client: KafkaAdminClient) -> None:
    """
    List all topics in the cluster.
    
    Args:
        admin_client: KafkaAdminClient instance
    """
    try:
        topics = admin_client.list_topics()
        logger.info(f"Existing topics: {sorted(topics)}")
    except Exception as e:
        logger.error(f"Error listing topics: {e}")
        raise
    finally:
        admin_client.close()


def main():
    parser = argparse.ArgumentParser(
        description='Create a Kafka topic on AWS MSK'
    )
    parser.add_argument(
        '--bootstrap-servers',
        required=True,
        help='Comma-separated list of bootstrap broker endpoints'
    )
    parser.add_argument(
        '--topic',
        default='demo-topic',
        help='Topic name to create (default: demo-topic)'
    )
    parser.add_argument(
        '--partitions',
        type=int,
        default=2,
        help='Number of partitions (default: 2)'
    )
    parser.add_argument(
        '--replication-factor',
        type=int,
        default=2,
        help='Replication factor (default: 2)'
    )
    parser.add_argument(
        '--region',
        default='us-east-1',
        help='AWS region (default: us-east-1)'
    )
    parser.add_argument(
        '--list-only',
        action='store_true',
        help='Only list existing topics without creating'
    )
    
    args = parser.parse_args()
    
    logger.info("Starting Kafka admin client...")
    logger.info(f"Bootstrap servers: {args.bootstrap_servers}")
    logger.info(f"Region: {args.region}")
    
    try:
        admin_client = create_admin_client(args.bootstrap_servers, args.region)
        
        if args.list_only:
            list_topics(admin_client)
        else:
            logger.info(f"Creating topic '{args.topic}'...")
            logger.info(f"Partitions: {args.partitions}")
            logger.info(f"Replication factor: {args.replication_factor}")
            create_topic(
                admin_client,
                args.topic,
                args.partitions,
                args.replication_factor
            )
    except Exception as e:
        logger.error(f"Script failed: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
