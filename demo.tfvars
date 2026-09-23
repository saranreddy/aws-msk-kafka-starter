# Cost-Optimized Configuration for Short Demos
# This configuration minimizes costs while maintaining full functionality
# Perfect for: testing, learning, short demonstrations (15-30 minutes)
#
# Expected cost: ~$0.36 for a 15-minute demo
# Expected cost: ~$2-5 for a day of intermittent testing
#
# IMPORTANT: Run `terraform destroy` when finished to stop charges!

aws_region  = "us-east-1"
environment = "dev"
project_name = "msk-kafka-starter"

# VPC Configuration - minimum for MSK
vpc_cidr           = "10.0.0.0/16"
availability_zones = ["us-east-1a", "us-east-1b"]

# Cost Optimization: Single NAT Gateway
# Saves ~$32/month compared to per-AZ NAT gateways
# Trade-off: Single point of failure (acceptable for demos)
single_nat_gateway = true

# Kafka Topic Configuration - minimal for demos
kafka_topic_name               = "demo-topic"
kafka_topic_partitions         = 2
kafka_topic_replication_factor = 2
