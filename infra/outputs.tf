output "msk_cluster_arn" {
  description = "ARN of the MSK Serverless cluster"
  value       = aws_msk_serverless_cluster.main.arn
}

output "msk_bootstrap_brokers" {
  description = "MSK bootstrap brokers for IAM authentication"
  value       = aws_msk_serverless_cluster.main.bootstrap_brokers_sasl_iam
}

output "vpc_id" {
  description = "ID of the VPC"
  value       = aws_vpc.main.id
}

output "private_subnet_ids" {
  description = "IDs of the private subnets"
  value       = aws_subnet.private[*].id
}

output "public_subnet_ids" {
  description = "IDs of the public subnets"
  value       = aws_subnet.public[*].id
}

output "client_security_group_id" {
  description = "ID of the client security group"
  value       = aws_security_group.client.id
}

output "msk_client_role_arn" {
  description = "ARN of the IAM role for MSK clients"
  value       = aws_iam_role.msk_client.arn
}

output "aws_region" {
  description = "AWS region where resources are created"
  value       = var.aws_region
}

output "kafka_topic_name" {
  description = "Name of the Kafka topic"
  value       = var.kafka_topic_name
}
