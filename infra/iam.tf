data "aws_caller_identity" "current" {}

resource "aws_iam_policy" "msk_client" {
  name        = "${var.project_name}-msk-client-policy"
  description = "IAM policy for MSK client access"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "kafka-cluster:Connect",
          "kafka-cluster:DescribeCluster",
          "kafka-cluster:AlterCluster",
          "kafka-cluster:DescribeClusterDynamicConfiguration"
        ]
        Resource = [
          aws_msk_serverless_cluster.main.arn
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "kafka-cluster:*Topic*",
          "kafka-cluster:WriteData",
          "kafka-cluster:ReadData"
        ]
        Resource = [
          "arn:aws:kafka:${var.aws_region}:${data.aws_caller_identity.current.account_id}:topic/${aws_msk_serverless_cluster.main.cluster_name}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "kafka-cluster:AlterGroup",
          "kafka-cluster:DescribeGroup"
        ]
        Resource = [
          "arn:aws:kafka:${var.aws_region}:${data.aws_caller_identity.current.account_id}:group/${aws_msk_serverless_cluster.main.cluster_name}/*"
        ]
      }
    ]
  })
}

resource "aws_iam_role" "msk_client" {
  name = "${var.project_name}-msk-client-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      },
      {
        Effect = "Allow"
        Principal = {
          AWS = data.aws_caller_identity.current.arn
        }
        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = {
    Name = "${var.project_name}-msk-client-role"
  }
}

resource "aws_iam_role_policy_attachment" "msk_client" {
  role       = aws_iam_role.msk_client.name
  policy_arn = aws_iam_policy.msk_client.arn
}

resource "aws_iam_instance_profile" "msk_client" {
  name = "${var.project_name}-msk-client-profile"
  role = aws_iam_role.msk_client.name
}
