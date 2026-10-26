output "console_url" {
  description = "Open in a browser (console); the same URL is the host for dynamoredis.redis.Redis(host=...)"
  value       = aws_lambda_function_url.server.function_url
}

output "table" {
  value = aws_dynamodb_table.kv.name
}
