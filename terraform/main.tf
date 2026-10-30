##############################################################################
# Real-AWS resources for hookrelay.
#
# This mirrors exactly what scripts/provision.ts creates against Moto, so the
# local and deployed topologies differ only in the endpoint they point at.
#
#   terraform init
#   terraform plan  -var="region=us-east-1"
#   terraform apply -var="region=us-east-1"
#
# Nothing here is created by CI; apply is a deliberate operator action.
##############################################################################

terraform {
  required_version = ">= 1.5"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.region

  default_tags {
    tags = {
      Project     = var.project
      Environment = var.environment
      ManagedBy   = "terraform"
    }
  }
}

locals {
  name_prefix = "${var.project}-${var.environment}"
}

# ---------------------------------------------------------------------------
# SQS: main queue + dead-letter queue with a redrive policy.
# ---------------------------------------------------------------------------

resource "aws_sqs_queue" "dlq" {
  name                       = "${local.name_prefix}-deliveries-dlq"
  message_retention_seconds  = 1209600 # 14 days, the SQS maximum
  visibility_timeout_seconds = 30
}

resource "aws_sqs_queue" "deliveries" {
  name                       = "${local.name_prefix}-deliveries"
  visibility_timeout_seconds = 30
  message_retention_seconds  = 345600 # 4 days

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dlq.arn
    maxReceiveCount     = 5
  })
}

# ---------------------------------------------------------------------------
# DynamoDB: one table per entity, on-demand capacity (no capacity planning and
# no idle cost for a low-traffic service).
# ---------------------------------------------------------------------------

resource "aws_dynamodb_table" "endpoints" {
  name         = "hookrelay_endpoints"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "id"

  attribute {
    name = "id"
    type = "S"
  }

  point_in_time_recovery {
    enabled = true
  }

  server_side_encryption {
    enabled = true
  }
}

resource "aws_dynamodb_table" "deliveries" {
  name         = "hookrelay_deliveries"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "id"

  attribute {
    name = "id"
    type = "S"
  }

  attribute {
    name = "endpointId"
    type = "S"
  }

  attribute {
    name = "createdAt"
    type = "S"
  }

  # Access pattern that would need this GSI first: "all deliveries for one
  # endpoint, newest first" (a per-endpoint delivery log in the dashboard).
  global_secondary_index {
    name            = "endpointId-createdAt"
    hash_key        = "endpointId"
    range_key       = "createdAt"
    projection_type = "ALL"
  }

  point_in_time_recovery {
    enabled = true
  }

  server_side_encryption {
    enabled = true
  }
}

# ---------------------------------------------------------------------------
# IAM: least privilege. The task role can only touch these two queues and two
# tables — no wildcard resources.
# ---------------------------------------------------------------------------

data "aws_iam_policy_document" "assume_role" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "task" {
  name               = "${local.name_prefix}-task"
  assume_role_policy = data.aws_iam_policy_document.assume_role.json
}

data "aws_iam_policy_document" "task" {
  statement {
    sid    = "SqsDeliveries"
    effect = "Allow"
    actions = [
      "sqs:SendMessage",
      "sqs:ReceiveMessage",
      "sqs:DeleteMessage",
      "sqs:GetQueueAttributes",
      "sqs:ChangeMessageVisibility",
    ]
    resources = [
      aws_sqs_queue.deliveries.arn,
      aws_sqs_queue.dlq.arn,
    ]
  }

  statement {
    sid    = "DynamoTables"
    effect = "Allow"
    actions = [
      "dynamodb:GetItem",
      "dynamodb:PutItem",
      "dynamodb:UpdateItem",
      "dynamodb:Query",
      "dynamodb:Scan",
    ]
    resources = [
      aws_dynamodb_table.endpoints.arn,
      aws_dynamodb_table.deliveries.arn,
      "${aws_dynamodb_table.deliveries.arn}/index/*",
    ]
  }
}

resource "aws_iam_role_policy" "task" {
  name   = "${local.name_prefix}-task"
  role   = aws_iam_role.task.id
  policy = data.aws_iam_policy_document.task.json
}
