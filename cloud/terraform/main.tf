provider "aws" {
  region = var.aws_region
  default_tags {
    tags = {
      Project     = var.project_name
      ManagedBy   = "Terraform"
      Environment = "tech-challenge"
    }
  }
}

resource "random_id" "suffix" {
  byte_length = 4
}

locals {
  name   = "${var.project_name}-${random_id.suffix.hex}"
  bucket = "${local.name}-lake"
}

resource "aws_s3_bucket" "lake" {
  bucket        = local.bucket
  force_destroy = false
}

resource "aws_s3_bucket_versioning" "lake" {
  bucket = aws_s3_bucket.lake.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "lake" {
  bucket                  = aws_s3_bucket.lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    id     = "abort-incomplete-multipart"
    status = "Enabled"
    filter {}
    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
  rule {
    id     = "expire-noncurrent-versions-after-90-days"
    status = "Enabled"
    filter {}
    noncurrent_version_expiration {
      noncurrent_days = 90
    }
  }
}

resource "aws_sqs_queue" "events" {
  name                       = "${local.name}-events"
  message_retention_seconds  = 86400
  visibility_timeout_seconds = 360
  receive_wait_time_seconds  = 20
  sqs_managed_sse_enabled    = true
}

resource "aws_sns_topic" "alerts" {
  name = "${local.name}-alerts"
}

resource "aws_sns_topic_subscription" "email" {
  count     = var.alert_email == "" ? 0 : 1
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

# Lambda: persists SQS events in S3 and emits event-latency metrics.
data "archive_file" "stream_lambda" {
  type        = "zip"
  source_file = "${path.module}/../lambda/stream_to_s3.py"
  output_path = "${path.module}/../lambda/function.zip"
}

resource "aws_iam_role" "lambda" {
  name = "${local.name}-stream-lambda-role"
  assume_role_policy = jsonencode({
    Version   = "2012-10-17",
    Statement = [{ Effect = "Allow", Principal = { Service = "lambda.amazonaws.com" }, Action = "sts:AssumeRole" }]
  })
}

resource "aws_iam_role_policy" "lambda" {
  name = "${local.name}-stream-lambda-policy"
  role = aws_iam_role.lambda.id
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      { Effect = "Allow", Action = ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes", "sqs:ChangeMessageVisibility"], Resource = aws_sqs_queue.events.arn },
      { Effect = "Allow", Action = ["s3:PutObject", "s3:AbortMultipartUpload"], Resource = "${aws_s3_bucket.lake.arn}/bronze/streaming/*" },
      { Effect = "Allow", Action = ["cloudwatch:PutMetricData"], Resource = "*" },
      { Effect = "Allow", Action = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"], Resource = "arn:aws:logs:${var.aws_region}:*:log-group:/aws/lambda/${local.name}-stream-consumer:*" }
    ]
  })
}

resource "aws_lambda_function" "stream_consumer" {
  function_name    = "${local.name}-stream-consumer"
  role             = aws_iam_role.lambda.arn
  handler          = "stream_to_s3.lambda_handler"
  runtime          = "python3.12"
  timeout          = 60
  memory_size      = 256
  filename         = data.archive_file.stream_lambda.output_path
  source_code_hash = data.archive_file.stream_lambda.output_base64sha256
  environment {
    variables = {
      BUCKET_NAME   = aws_s3_bucket.lake.bucket
      STREAM_PREFIX = "bronze/streaming"
    }
  }
  depends_on = [aws_iam_role_policy.lambda]
}

resource "aws_lambda_event_source_mapping" "sqs" {
  event_source_arn                   = aws_sqs_queue.events.arn
  function_name                      = aws_lambda_function.stream_consumer.arn
  batch_size                         = 10
  enabled                            = false
  maximum_batching_window_in_seconds = 0
}

resource "aws_cloudwatch_metric_alarm" "lambda_errors" {
  alarm_name          = "${local.name}-stream-lambda-errors"
  alarm_description   = "Alerta quando o consumidor de streaming apresenta erro."
  namespace           = "AWS/Lambda"
  metric_name         = "Errors"
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"
  dimensions          = { FunctionName = aws_lambda_function.stream_consumer.function_name }
  alarm_actions       = [aws_sns_topic.alerts.arn]
}

# Glue job: processes the six canonical CSVs into run-versioned Silver and Gold Parquet.
resource "aws_iam_role" "glue" {
  name = "${local.name}-glue-role"
  assume_role_policy = jsonencode({
    Version   = "2012-10-17",
    Statement = [{ Effect = "Allow", Principal = { Service = "glue.amazonaws.com" }, Action = "sts:AssumeRole" }]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue_s3" {
  name = "${local.name}-glue-s3-policy"
  role = aws_iam_role.glue.id
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      { Effect = "Allow", Action = ["s3:ListBucket", "s3:GetBucketLocation"], Resource = aws_s3_bucket.lake.arn },
      { Effect = "Allow", Action = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"], Resource = "${aws_s3_bucket.lake.arn}/*" }
    ]
  })
}

resource "aws_s3_object" "glue_script" {
  bucket       = aws_s3_bucket.lake.id
  key          = "scripts/glue_medallion_job.py"
  source       = "${path.module}/../../jobs/glue_medallion_job.py"
  etag         = filemd5("${path.module}/../../jobs/glue_medallion_job.py")
  content_type = "text/x-python"
}

resource "aws_glue_job" "pipeline" {
  name              = "${local.name}-medallion-pipeline"
  role_arn          = aws_iam_role.glue.arn
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 30
  max_retries       = 0
  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.lake.bucket}/${aws_s3_object.glue_script.key}"
    python_version  = "3"
  }
  default_arguments = {
    "--SOURCE_PREFIX"                    = "s3://${aws_s3_bucket.lake.bucket}/bronze/source"
    "--STREAM_PREFIX"                    = "s3://${aws_s3_bucket.lake.bucket}/bronze/streaming"
    "--OUTPUT_PREFIX"                    = "s3://${aws_s3_bucket.lake.bucket}/silver"
    "--GOLD_PREFIX"                      = "s3://${aws_s3_bucket.lake.bucket}/gold"
    "--MIN_JOIN_MATCH_RATE"              = tostring(var.min_join_match_rate)
    "--MAX_INVALID_WEIGHT_RATE"          = tostring(var.max_invalid_weight_rate)
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-spark-ui"                  = "true"
    "--TempDir"                          = "s3://${aws_s3_bucket.lake.bucket}/tmp/glue/"
  }
  execution_property {
    max_concurrent_runs = 1
  }
  depends_on = [aws_iam_role_policy_attachment.glue_service, aws_iam_role_policy.glue_s3, aws_s3_object.glue_script]
}

resource "aws_iam_role" "step_functions" {
  name = "${local.name}-sfn-role"
  assume_role_policy = jsonencode({
    Version   = "2012-10-17",
    Statement = [{ Effect = "Allow", Principal = { Service = "states.amazonaws.com" }, Action = "sts:AssumeRole" }]
  })
}

resource "aws_iam_role_policy" "step_functions" {
  name = "${local.name}-sfn-glue-policy"
  role = aws_iam_role.step_functions.id
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      { Effect = "Allow", Action = ["glue:StartJobRun", "glue:GetJobRun", "glue:GetJobRuns", "glue:BatchStopJobRun"], Resource = aws_glue_job.pipeline.arn },
      { Effect = "Allow", Action = ["events:PutTargets", "events:PutRule", "events:DescribeRule"], Resource = "arn:aws:events:${var.aws_region}:*:rule/StepFunctionsGetEventsForGlueJobRunTaskRule" }
    ]
  })
}

resource "aws_sfn_state_machine" "pipeline" {
  name     = "${local.name}-pipeline-orchestrator"
  role_arn = aws_iam_role.step_functions.arn
  definition = jsonencode({
    Comment = "Executa o job Glue da pipeline híbrida de alfabetização.",
    StartAt = "RunMedallionGlueJob",
    States = {
      RunMedallionGlueJob = {
        Type       = "Task",
        Resource   = "arn:aws:states:::glue:startJobRun.sync",
        Parameters = { JobName = aws_glue_job.pipeline.name },
        Retry      = [{ ErrorEquals = ["Glue.ConcurrentRunsExceededException", "States.TaskFailed"], IntervalSeconds = 30, MaxAttempts = 2, BackoffRate = 2.0 }],
        Catch      = [{ ErrorEquals = ["States.ALL"], ResultPath = "$.error", Next = "PipelineFailed" }],
        Next       = "PipelineSucceeded"
      },
      PipelineSucceeded = { Type = "Succeed" },
      PipelineFailed    = { Type = "Fail", Error = "PipelineExecutionFailed", Cause = "Consulte os logs do job Glue e o relatório de qualidade." }
    }
  })
  depends_on = [aws_iam_role_policy.step_functions]
}

resource "aws_cloudwatch_event_rule" "daily" {
  count               = var.enable_daily_schedule ? 1 : 0
  name                = "${local.name}-daily-run"
  description         = "Agenda diária do pipeline; desabilitada por padrão até validação manual."
  schedule_expression = "rate(1 day)"
}

resource "aws_iam_role" "eventbridge_sfn" {
  count = var.enable_daily_schedule ? 1 : 0
  name  = "${local.name}-eventbridge-sfn-role"
  assume_role_policy = jsonencode({
    Version   = "2012-10-17",
    Statement = [{ Effect = "Allow", Principal = { Service = "events.amazonaws.com" }, Action = "sts:AssumeRole" }]
  })
}

resource "aws_iam_role_policy" "eventbridge_sfn" {
  count  = var.enable_daily_schedule ? 1 : 0
  name   = "${local.name}-eventbridge-sfn-policy"
  role   = aws_iam_role.eventbridge_sfn[0].id
  policy = jsonencode({ Version = "2012-10-17", Statement = [{ Effect = "Allow", Action = ["states:StartExecution"], Resource = aws_sfn_state_machine.pipeline.arn }] })
}

resource "aws_cloudwatch_event_target" "daily_sfn" {
  count      = var.enable_daily_schedule ? 1 : 0
  rule       = aws_cloudwatch_event_rule.daily[0].name
  target_id  = "StartAlfabetizacaoPipeline"
  arn        = aws_sfn_state_machine.pipeline.arn
  role_arn   = aws_iam_role.eventbridge_sfn[0].arn
  depends_on = [aws_iam_role_policy.eventbridge_sfn]
}

resource "aws_glue_catalog_database" "analytics" {
  name        = "${replace(local.name, "-", "_")}_catalog"
  description = "Catálogo do Data Lake do Tech Challenge de Alfabetização."
}

resource "aws_glue_crawler" "silver" {
  name          = "${local.name}-silver-crawler"
  database_name = aws_glue_catalog_database.analytics.name
  role          = aws_iam_role.glue.arn
  s3_target {
    path = "s3://${aws_s3_bucket.lake.bucket}/silver/"
  }
  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior = "UPDATE_IN_DATABASE"
  }
  depends_on = [aws_iam_role_policy_attachment.glue_service, aws_iam_role_policy.glue_s3]
}

resource "aws_glue_crawler" "gold" {
  name          = "${local.name}-gold-crawler"
  database_name = aws_glue_catalog_database.analytics.name
  role          = aws_iam_role.glue.arn
  s3_target {
    path = "s3://${aws_s3_bucket.lake.bucket}/gold/"
  }
  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior = "UPDATE_IN_DATABASE"
  }
  depends_on = [aws_iam_role_policy_attachment.glue_service, aws_iam_role_policy.glue_s3]
}

resource "aws_athena_workgroup" "analytics" {
  name        = "${local.name}-analytics"
  description = "Workgroup Athena para consultar camadas Silver e Gold."
  state       = "ENABLED"
  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true
    result_configuration {
      output_location = "s3://${aws_s3_bucket.lake.bucket}/athena-results/"
      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}

resource "aws_cloudwatch_event_rule" "pipeline_failed" {
  name        = "${local.name}-pipeline-failed"
  description = "Identifica falha ou timeout da execução Step Functions."
  event_pattern = jsonencode({
    source        = ["aws.states"],
    "detail-type" = ["Step Functions Execution Status Change"],
    detail = {
      status          = ["FAILED", "TIMED_OUT", "ABORTED"],
      stateMachineArn = [aws_sfn_state_machine.pipeline.arn]
    }
  })
}

resource "aws_sns_topic_policy" "alerts" {
  arn = aws_sns_topic.alerts.arn
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Sid       = "AllowAccountOwner",
        Effect    = "Allow",
        Principal = { AWS = "*" },
        Action    = ["SNS:GetTopicAttributes", "SNS:SetTopicAttributes", "SNS:AddPermission", "SNS:RemovePermission", "SNS:DeleteTopic", "SNS:Subscribe", "SNS:ListSubscriptionsByTopic", "SNS:Publish"],
        Resource  = aws_sns_topic.alerts.arn,
        Condition = { StringEquals = { "AWS:SourceOwner" = data.aws_caller_identity.current.account_id } }
      },
      {
        Sid       = "AllowEventBridgePublish",
        Effect    = "Allow",
        Principal = { Service = "events.amazonaws.com" },
        Action    = "sns:Publish",
        Resource  = aws_sns_topic.alerts.arn,
        Condition = { ArnEquals = { "aws:SourceArn" = aws_cloudwatch_event_rule.pipeline_failed.arn } }
      }
    ]
  })
}

data "aws_caller_identity" "current" {}

resource "aws_cloudwatch_event_target" "pipeline_failed_sns" {
  rule       = aws_cloudwatch_event_rule.pipeline_failed.name
  target_id  = "NotifyPipelineFailure"
  arn        = aws_sns_topic.alerts.arn
  depends_on = [aws_sns_topic_policy.alerts]
}
