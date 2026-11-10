terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}
variable "env" {
  default = "dev"
}
variable "region" {
  default = "us-east-1"
}
variable "subnet_ids" {
  type    = list(string)
  default = [] # set per VPC before any plan; this skeleton is validate-only
}
provider "aws" {
  region = var.region
}

# Device shadows: conditional writes on `version` give optimistic concurrency.
resource "aws_dynamodb_table" "shadows" {
  name         = "fleet-shadows-${var.env}"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "device_id"
  attribute {
    name = "device_id"
    type = "S"
  }
}

resource "aws_sqs_queue" "dlq" {
  name = "fleet-config-dlq-${var.env}"
}
resource "aws_sqs_queue" "config" {
  name                       = "fleet-config-${var.env}"
  visibility_timeout_seconds = 60
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dlq.arn
    maxReceiveCount     = 3
  })
}

# ECS skeleton (EKS is interchangeable here; ECS is less YAML for the same shape).
resource "aws_ecs_cluster" "fleet" {
  name = "fleet-${var.env}"
}
resource "aws_ecs_task_definition" "api" {
  family                   = "fleet-api"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = 512
  memory                   = 1024
  container_definitions = jsonencode([{
    name         = "api"
    image        = "fleet-api:latest"
    essential    = true
    portMappings = [{ containerPort = 8080 }]
    environment  = [{ name = "ADDR", value = ":8080" }]
  }])
}
resource "aws_ecs_service" "api" {
  name            = "fleet-api"
  cluster         = aws_ecs_cluster.fleet.id
  task_definition = aws_ecs_task_definition.api.arn
  desired_count   = 2
  launch_type     = "FARGATE"
  network_configuration {
    subnets          = var.subnet_ids
    assign_public_ip = true
  }
}

output "shadow_table" {
  value = aws_dynamodb_table.shadows.name
}
output "config_queue" {
  value = aws_sqs_queue.config.url
}
output "config_dlq" {
  value = aws_sqs_queue.dlq.url
}
