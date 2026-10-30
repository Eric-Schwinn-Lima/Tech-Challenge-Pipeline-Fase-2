output "s3_bucket_name" {
  value       = aws_s3_bucket.lake.bucket
  description = "Bucket do Data Lake."
}

output "sqs_queue_name" {
  value       = aws_sqs_queue.events.name
  description = "Nome da fila SQS de eventos simulados."
}

output "sqs_queue_url" {
  value       = aws_sqs_queue.events.url
  description = "URL da fila SQS de eventos simulados."
}

output "glue_job_name" {
  value       = aws_glue_job.pipeline.name
  description = "Nome do job Glue para executar a ingestão/transformação na nuvem."
}

output "state_machine_arn" {
  value       = aws_sfn_state_machine.pipeline.arn
  description = "ARN da Step Functions para executar a pipeline."
}

output "sns_topic_arn" {
  value       = aws_sns_topic.alerts.arn
  description = "Tópico SNS de alertas. Configure/ confirme uma assinatura de e-mail se necessário."
}

output "glue_catalog_database" {
  value       = aws_glue_catalog_database.analytics.name
  description = "Database do Glue Data Catalog."
}

output "athena_workgroup" {
  value       = aws_athena_workgroup.analytics.name
  description = "Workgroup configurado no Athena."
}

output "silver_crawler_name" {
  value       = aws_glue_crawler.silver.name
  description = "Crawler do catálogo Silver."
}

output "gold_crawler_name" {
  value       = aws_glue_crawler.gold.name
  description = "Crawler do catálogo Gold."
}

output "bucket_arn" {
  value       = aws_s3_bucket.lake.arn
  description = "ARN do bucket de dados."
}
