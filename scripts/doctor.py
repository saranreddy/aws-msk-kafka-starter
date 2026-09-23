#!/usr/bin/env python3
"""
Doctor Script for AWS MSK Kafka Starter

This script performs comprehensive health checks for AWS MSK Kafka cluster setup,
including AWS credentials, region consistency, IAM permissions, Terraform state,
and MSK cluster status.
"""

import argparse
import json
import logging
import os
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Tuple

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class CheckResult:
    """Represents the result of a health check."""

    def __init__(self, name: str, passed: bool, message: str, fix: str = None):
        self.name = name
        self.passed = passed
        self.message = message
        self.fix = fix


class MSKDoctor:
    """Performs health checks for MSK Kafka setup."""

    def __init__(self, verbose: bool = False):
        self.verbose = verbose
        self.results: List[CheckResult] = []
        self.terraform_dir = Path(__file__).parent.parent / "infra"

    def run_command(self, cmd: List[str], check: bool = False) -> Tuple[int, str, str]:
        """Run a shell command and return exit code, stdout, stderr."""
        try:
            result = subprocess.run(
                cmd, capture_output=True, text=True, timeout=30, check=check
            )
            return result.returncode, result.stdout.strip(), result.stderr.strip()
        except subprocess.TimeoutExpired:
            return -1, "", "Command timed out"
        except subprocess.CalledProcessError as e:
            return e.returncode, e.stdout.strip(), e.stderr.strip()
        except FileNotFoundError:
            return -1, "", f"Command not found: {cmd[0]}"

    def add_result(self, name: str, passed: bool, message: str, fix: str = None):
        """Add a check result."""
        result = CheckResult(name, passed, message, fix)
        self.results.append(result)
        return result

    def check_aws_cli(self) -> CheckResult:
        """Check if AWS CLI is installed and accessible."""
        logger.info("Checking AWS CLI installation...")
        returncode, stdout, stderr = self.run_command(["aws", "--version"])

        if returncode == 0:
            return self.add_result(
                "AWS CLI", True, f"AWS CLI is installed: {stdout.split()[0]}"
            )
        else:
            return self.add_result(
                "AWS CLI",
                False,
                "AWS CLI not found",
                "Install AWS CLI: https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html",
            )

    def check_aws_credentials(self) -> CheckResult:
        """Check if AWS credentials are configured."""
        logger.info("Checking AWS credentials...")
        returncode, stdout, stderr = self.run_command(
            ["aws", "sts", "get-caller-identity"]
        )

        if returncode == 0:
            try:
                identity = json.loads(stdout)
                return self.add_result(
                    "AWS Credentials",
                    True,
                    f"Authenticated as: {identity.get('Arn', 'Unknown')}",
                )
            except json.JSONDecodeError:
                return self.add_result(
                    "AWS Credentials",
                    False,
                    "Could not parse AWS identity",
                    "Run: aws configure",
                )
        else:
            return self.add_result(
                "AWS Credentials",
                False,
                f"Not authenticated: {stderr}",
                "Run: aws configure",
            )

    def check_terraform(self) -> CheckResult:
        """Check if Terraform is installed."""
        logger.info("Checking Terraform installation...")
        returncode, stdout, stderr = self.run_command(["terraform", "--version"])

        if returncode == 0:
            return self.add_result(
                "Terraform", True, f"Terraform is installed: {stdout.split()[1]}"
            )
        else:
            return self.add_result(
                "Terraform",
                False,
                "Terraform not found",
                "Install Terraform: https://www.terraform.io/downloads",
            )

    def check_terraform_state(self) -> Tuple[CheckResult, Dict]:
        """Check if Terraform state exists and get outputs."""
        logger.info("Checking Terraform state...")

        if not self.terraform_dir.exists():
            return (
                self.add_result(
                    "Terraform State",
                    False,
                    f"Terraform directory not found: {self.terraform_dir}",
                ),
                {},
            )

        state_file = self.terraform_dir / "terraform.tfstate"
        if not state_file.exists():
            return (
                self.add_result(
                    "Terraform State",
                    False,
                    "No Terraform state found - infrastructure not deployed",
                    "Run: cd infra && terraform init && terraform apply",
                ),
                {},
            )

        # Try to get outputs
        returncode, stdout, stderr = self.run_command(
            ["terraform", "output", "-json"], check=False
        )

        if returncode != 0:
            return (
                self.add_result(
                    "Terraform State",
                    False,
                    f"Could not read Terraform outputs: {stderr}",
                ),
                {},
            )

        try:
            outputs = json.loads(stdout)
            output_keys = list(outputs.keys())
            return (
                self.add_result(
                    "Terraform State",
                    True,
                    f"Terraform state found with {len(output_keys)} outputs",
                ),
                outputs,
            )
        except json.JSONDecodeError:
            return (
                self.add_result(
                    "Terraform State", False, "Could not parse Terraform outputs"
                ),
                {},
            )

    def check_region_consistency(self, tf_outputs: Dict) -> CheckResult:
        """Check region consistency across AWS CLI, Terraform, and environment."""
        logger.info("Checking region consistency...")

        regions = {}

        # Get region from environment variable
        env_region = os.environ.get("AWS_REGION") or os.environ.get(
            "AWS_DEFAULT_REGION"
        )
        if env_region:
            regions["Environment"] = env_region

        # Get region from AWS CLI config
        returncode, stdout, stderr = self.run_command(
            ["aws", "configure", "get", "region"]
        )
        if returncode == 0 and stdout:
            regions["AWS CLI"] = stdout

        # Get region from Terraform outputs
        if tf_outputs and "aws_region" in tf_outputs:
            regions["Terraform"] = tf_outputs["aws_region"]["value"]

        if not regions:
            return self.add_result(
                "Region Consistency",
                False,
                "No AWS region configured",
                "Set AWS_REGION environment variable or configure AWS CLI default region",
            )

        # Check if all regions are the same
        unique_regions = set(regions.values())
        if len(unique_regions) == 1:
            region = list(unique_regions)[0]
            return self.add_result(
                "Region Consistency", True, f"All sources agree on region: {region}"
            )
        else:
            region_str = ", ".join([f"{k}={v}" for k, v in regions.items()])
            return self.add_result(
                "Region Consistency",
                False,
                f"Region mismatch: {region_str}",
                "Ensure AWS_REGION matches your Terraform configuration",
            )

    def check_terraform_outputs(self, tf_outputs: Dict) -> CheckResult:
        """Check if required Terraform outputs are present."""
        logger.info("Checking Terraform outputs...")

        required_outputs = [
            "msk_cluster_arn",
            "msk_bootstrap_brokers",
            "kafka_topic_name",
            "aws_region",
        ]

        if not tf_outputs:
            return self.add_result(
                "Terraform Outputs",
                False,
                "No Terraform outputs available",
                "Deploy infrastructure: cd infra && terraform apply",
            )

        missing = [out for out in required_outputs if out not in tf_outputs]

        if missing:
            return self.add_result(
                "Terraform Outputs",
                False,
                f"Missing outputs: {', '.join(missing)}",
                "Run: cd infra && terraform apply",
            )
        else:
            return self.add_result(
                "Terraform Outputs",
                True,
                f"All required outputs present: {', '.join(required_outputs)}",
            )

    def check_bootstrap_string(self, tf_outputs: Dict) -> CheckResult:
        """Validate bootstrap servers string format."""
        logger.info("Checking bootstrap servers format...")

        if not tf_outputs or "msk_bootstrap_brokers" not in tf_outputs:
            return self.add_result(
                "Bootstrap String", False, "Bootstrap servers not available"
            )

        bootstrap = tf_outputs["msk_bootstrap_brokers"]["value"]

        if not bootstrap:
            return self.add_result(
                "Bootstrap String",
                False,
                "Bootstrap servers string is empty",
                "Wait for MSK cluster to become ACTIVE",
            )

        # MSK Serverless bootstrap format: boot-xxxxx.yyyy.kafka-serverless.region.amazonaws.com:9098
        if ".kafka-serverless." not in bootstrap or ":9098" not in bootstrap:
            return self.add_result(
                "Bootstrap String",
                False,
                f"Bootstrap string doesn't match MSK Serverless format: {bootstrap}",
            )

        broker_count = len(bootstrap.split(","))
        return self.add_result(
            "Bootstrap String",
            True,
            f"Bootstrap string valid with {broker_count} broker(s)",
        )

    def check_msk_cluster_status(self, tf_outputs: Dict) -> CheckResult:
        """Check if MSK cluster is in ACTIVE state."""
        logger.info("Checking MSK cluster status...")

        if not tf_outputs or "msk_cluster_arn" not in tf_outputs:
            return self.add_result(
                "MSK Cluster Status", False, "MSK cluster ARN not available"
            )

        cluster_arn = tf_outputs["msk_cluster_arn"]["value"]
        region = tf_outputs.get("aws_region", {}).get("value", "us-east-1")

        returncode, stdout, stderr = self.run_command(
            [
                "aws",
                "kafka",
                "describe-cluster-v2",
                "--cluster-arn",
                cluster_arn,
                "--region",
                region,
            ]
        )

        if returncode != 0:
            return self.add_result(
                "MSK Cluster Status",
                False,
                f"Could not describe cluster: {stderr}",
                "Check IAM permissions for kafka:DescribeClusterV2",
            )

        try:
            cluster_info = json.loads(stdout)
            state = cluster_info.get("ClusterInfo", {}).get("State", "UNKNOWN")

            if state == "ACTIVE":
                return self.add_result(
                    "MSK Cluster Status", True, f"MSK cluster is {state}"
                )
            else:
                return self.add_result(
                    "MSK Cluster Status",
                    False,
                    f"MSK cluster is {state}, expected ACTIVE",
                    "Wait for cluster to become ACTIVE or check AWS Console for errors",
                )
        except json.JSONDecodeError:
            return self.add_result(
                "MSK Cluster Status", False, "Could not parse cluster information"
            )

    def check_iam_permissions(self, tf_outputs: Dict) -> CheckResult:
        """Check basic IAM permissions for MSK operations."""
        logger.info("Checking IAM permissions...")

        if not tf_outputs or "msk_cluster_arn" not in tf_outputs:
            return self.add_result(
                "IAM Permissions",
                False,
                "Cannot check IAM permissions without cluster ARN",
            )

        cluster_arn = tf_outputs["msk_cluster_arn"]["value"]
        region = tf_outputs.get("aws_region", {}).get("value", "us-east-1")

        # Try to describe cluster as a basic permission check
        returncode, stdout, stderr = self.run_command(
            [
                "aws",
                "kafka",
                "describe-cluster-v2",
                "--cluster-arn",
                cluster_arn,
                "--region",
                region,
            ]
        )

        if returncode == 0:
            return self.add_result(
                "IAM Permissions",
                True,
                "Basic IAM permissions verified (kafka:DescribeClusterV2)",
            )
        elif "AccessDeniedException" in stderr or "UnauthorizedOperation" in stderr:
            return self.add_result(
                "IAM Permissions",
                False,
                "Missing IAM permissions for MSK operations",
                "Ensure your IAM user/role has the MSK client policy attached",
            )
        else:
            return self.add_result(
                "IAM Permissions",
                False,
                f"Could not verify IAM permissions: {stderr}",
            )

    def check_network_access(self, tf_outputs: Dict) -> CheckResult:
        """Check network access notes and connectivity."""
        logger.info("Checking network access...")

        if not tf_outputs or "msk_bootstrap_brokers" not in tf_outputs:
            return self.add_result(
                "Network Access",
                False,
                "Cannot check network without bootstrap servers",
            )

        bootstrap = tf_outputs["msk_bootstrap_brokers"]["value"]

        # MSK Serverless uses private endpoints
        message = (
            "MSK bootstrap brokers use private VPC endpoints (port 9098). "
            "Access requires:\n"
            "    • Running from within the VPC (same VPC as MSK cluster)\n"
            "    • OR using AWS Systems Manager Session Manager (SSM)\n"
            "    • OR using a bastion host in the VPC\n"
            "    • OR using VPN/Direct Connect to the VPC\n\n"
            "    Local laptop/desktop cannot directly connect without network path."
        )

        # Try to get VPC ID
        vpc_info = ""
        if "vpc_id" in tf_outputs:
            vpc_id = tf_outputs["vpc_id"]["value"]
            vpc_info = f"\n    VPC ID: {vpc_id}"

        return self.add_result(
            "Network Access",
            True,
            message + vpc_info,
            "For testing: Deploy scripts on EC2 instance in same VPC, or use SSM Session Manager port forwarding",
        )

    def check_python_dependencies(self) -> CheckResult:
        """Check if required Python packages are installed."""
        logger.info("Checking Python dependencies...")

        try:
            import boto3  # noqa: F401
            import kafka  # noqa: F401

            return self.add_result(
                "Python Dependencies",
                True,
                "Required Python packages installed (kafka-python-ng, boto3)",
            )
        except ImportError as e:
            return self.add_result(
                "Python Dependencies",
                False,
                f"Missing Python dependencies: {e}",
                "Run: pip install -r requirements.txt",
            )

    def run_all_checks(self) -> bool:
        """Run all health checks."""
        logger.info("Starting MSK Kafka health checks...\n")

        # Basic prerequisite checks
        self.check_aws_cli()
        self.check_aws_credentials()
        self.check_terraform()
        self.check_python_dependencies()

        # Terraform and infrastructure checks
        _, tf_outputs = self.check_terraform_state()

        if tf_outputs:
            self.check_region_consistency(tf_outputs)
            self.check_terraform_outputs(tf_outputs)
            self.check_bootstrap_string(tf_outputs)
            self.check_msk_cluster_status(tf_outputs)
            self.check_iam_permissions(tf_outputs)
            self.check_network_access(tf_outputs)
        else:
            self.add_result(
                "Infrastructure Checks",
                False,
                "Skipping infrastructure checks - no Terraform state",
                "Deploy infrastructure first: cd infra && terraform init && terraform apply",
            )

        return all(result.passed for result in self.results)

    def print_results(self):
        """Print check results in a formatted manner."""
        print("\n" + "=" * 80)
        print("AWS MSK Kafka Doctor - Health Check Results")
        print("=" * 80 + "\n")

        passed_count = sum(1 for r in self.results if r.passed)
        total_count = len(self.results)

        for result in self.results:
            status = "✅ PASS" if result.passed else "❌ FAIL"
            print(f"{status} - {result.name}")
            print(f"    {result.message}")

            if not result.passed and result.fix:
                print(f"    💡 Fix: {result.fix}")
            print()

        print("=" * 80)
        print(f"Summary: {passed_count}/{total_count} checks passed")
        print("=" * 80 + "\n")

        if passed_count == total_count:
            print("✅ All checks passed! Your MSK Kafka setup is healthy.")
            return 0
        else:
            print(
                "❌ Some checks failed. Please address the issues above before proceeding."
            )
            return 1


def main():
    parser = argparse.ArgumentParser(
        description="Health check for AWS MSK Kafka starter setup"
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Enable verbose output"
    )

    args = parser.parse_args()

    doctor = MSKDoctor(verbose=args.verbose)

    try:
        # Change to terraform directory for commands
        original_dir = os.getcwd()
        os.chdir(doctor.terraform_dir)

        all_passed = doctor.run_all_checks()

        # Change back to original directory
        os.chdir(original_dir)

        exit_code = doctor.print_results()
        sys.exit(exit_code)

    except KeyboardInterrupt:
        print("\n\nDoctor check interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        if args.verbose:
            import traceback

            traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
