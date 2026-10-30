output "queue_url" {
  description = "URL of the main delivery queue (set as SQS_QUEUE_URL)."
  value       = aws_sqs_queue.deliveries.url
}

output "dlq_url" {
  description = "URL of the dead-letter queue."
  value       = aws_sqs_queue.dlq.url
}

output "queue_arn" {
  description = "ARN of the main delivery queue."
  value       = aws_sqs_queue.deliveries.arn
}

output "endpoints_table" {
  description = "DynamoDB table holding webhook endpoints."
  value       = aws_dynamodb_table.endpoints.name
}

output "deliveries_table" {
  description = "DynamoDB table holding delivery state."
  value       = aws_dynamodb_table.deliveries.name
}

output "task_role_arn" {
  description = "IAM role to attach to the API and worker tasks."
  value       = aws_iam_role.task.arn
}
