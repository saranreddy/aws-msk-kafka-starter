#!/bin/bash
#
# Quick setup script for AWS MSK Kafka Starter
# This script helps you get started quickly by:
# 1. Checking prerequisites
# 2. Deploying infrastructure
# 3. Exporting connection details
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "=========================================="
echo "AWS MSK Kafka Starter - Quick Setup"
echo "=========================================="
echo ""

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check prerequisites
echo "Checking prerequisites..."

# Check AWS CLI
if ! command -v aws &> /dev/null; then
    echo -e "${RED}✗ AWS CLI not found${NC}"
    echo "  Install: https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html"
    exit 1
fi
echo -e "${GREEN}✓ AWS CLI installed${NC}"

# Check Terraform
if ! command -v terraform &> /dev/null; then
    echo -e "${RED}✗ Terraform not found${NC}"
    echo "  Install: https://www.terraform.io/downloads"
    exit 1
fi
echo -e "${GREEN}✓ Terraform installed${NC}"

# Check Python
if ! command -v python3 &> /dev/null; then
    echo -e "${RED}✗ Python 3 not found${NC}"
    echo "  Install: https://www.python.org/downloads/"
    exit 1
fi
echo -e "${GREEN}✓ Python 3 installed${NC}"

# Check AWS credentials
if ! aws sts get-caller-identity &> /dev/null; then
    echo -e "${RED}✗ AWS credentials not configured${NC}"
    echo "  Run: aws configure"
    exit 1
fi
echo -e "${GREEN}✓ AWS credentials configured${NC}"

echo ""
echo "All prerequisites met!"
echo ""

# Install Python dependencies
echo "Installing Python dependencies..."
pip install -q -r "$PROJECT_ROOT/requirements.txt"
echo -e "${GREEN}✓ Python dependencies installed${NC}"
echo ""

# Deploy infrastructure
echo "=========================================="
echo "Deploying Infrastructure with Terraform"
echo "=========================================="
echo ""
echo -e "${YELLOW}WARNING: This will create AWS resources that incur charges.${NC}"
echo -e "${YELLOW}Estimated cost: ~\$542-670/month if left running.${NC}"
echo ""
read -p "Do you want to continue? (yes/no): " -r
if [[ ! $REPLY =~ ^[Yy][Ee][Ss]$ ]]; then
    echo "Setup cancelled."
    exit 0
fi

cd "$PROJECT_ROOT/infra"

echo ""
echo "Initializing Terraform..."
terraform init

echo ""
echo "Planning infrastructure..."
terraform plan -out=tfplan

echo ""
read -p "Apply this plan? (yes/no): " -r
if [[ ! $REPLY =~ ^[Yy][Ee][Ss]$ ]]; then
    echo "Setup cancelled."
    exit 0
fi

echo ""
echo "Applying infrastructure (this takes ~5-10 minutes)..."
terraform apply tfplan

echo ""
echo -e "${GREEN}✓ Infrastructure deployed successfully!${NC}"
echo ""

# Export environment variables
echo "=========================================="
echo "Connection Details"
echo "=========================================="
echo ""

BOOTSTRAP_SERVERS=$(terraform output -raw msk_bootstrap_brokers)
KAFKA_TOPIC=$(terraform output -raw kafka_topic_name)
AWS_REGION=$(terraform output -raw aws_region)

echo "Bootstrap Servers: $BOOTSTRAP_SERVERS"
echo "Kafka Topic: $KAFKA_TOPIC"
echo "AWS Region: $AWS_REGION"
echo ""

# Create exports file
cat > "$PROJECT_ROOT/.env.msk" <<EOF
export BOOTSTRAP_SERVERS="$BOOTSTRAP_SERVERS"
export KAFKA_TOPIC="$KAFKA_TOPIC"
export AWS_REGION="$AWS_REGION"
EOF

echo -e "${GREEN}Environment variables saved to .env.msk${NC}"
echo "To use them, run: source .env.msk"
echo ""

# Create Kafka topic
echo "=========================================="
echo "Creating Kafka Topic"
echo "=========================================="
echo ""

cd "$SCRIPT_DIR"
python3 create_topic.py \
    --bootstrap-servers "$BOOTSTRAP_SERVERS" \
    --topic "$KAFKA_TOPIC" \
    --region "$AWS_REGION"

echo ""
echo -e "${GREEN}✓ Kafka topic created successfully!${NC}"
echo ""

# Final instructions
echo "=========================================="
echo "Setup Complete!"
echo "=========================================="
echo ""
echo "Next steps:"
echo ""
echo "1. Source the environment variables:"
echo "   source .env.msk"
echo ""
echo "2. Produce some messages:"
echo "   python3 scripts/produce.py \\"
echo "     --bootstrap-servers \"\$BOOTSTRAP_SERVERS\" \\"
echo "     --topic \"\$KAFKA_TOPIC\" \\"
echo "     --region \"\$AWS_REGION\""
echo ""
echo "3. Consume messages (in another terminal):"
echo "   python3 scripts/consume.py \\"
echo "     --bootstrap-servers \"\$BOOTSTRAP_SERVERS\" \\"
echo "     --topic \"\$KAFKA_TOPIC\" \\"
echo "     --from-beginning \\"
echo "     --region \"\$AWS_REGION\""
echo ""
echo -e "${YELLOW}Remember to destroy resources when done to avoid charges:${NC}"
echo "   cd infra && terraform destroy"
echo ""
