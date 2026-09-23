variable "aws_region" {
  description = "AWS region for all resources"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Environment name (e.g., dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "project_name" {
  description = "Project name used for resource naming"
  type        = string
  default     = "msk-kafka-starter"
}

variable "vpc_cidr" {
  description = "CIDR block for VPC"
  type        = string
  default     = "10.0.0.0/16"
}

variable "availability_zones" {
  description = "List of availability zones to use (must be at least 2 for MSK)"
  type        = list(string)
  default     = ["us-east-1a", "us-east-1b"]
}

variable "kafka_topic_name" {
  description = "Name of the Kafka topic to create"
  type        = string
  default     = "demo-topic"
}

variable "kafka_topic_partitions" {
  description = "Number of partitions for the Kafka topic"
  type        = number
  default     = 2
}

variable "kafka_topic_replication_factor" {
  description = "Replication factor for the Kafka topic"
  type        = number
  default     = 2
}
