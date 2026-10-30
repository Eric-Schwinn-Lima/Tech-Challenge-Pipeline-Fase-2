
variable "aws_region" {
  description = "Região AWS onde os recursos do projeto serão criados."
  type        = string
  default     = "us-east-2"
}

variable "project_name" {
  description = "Prefixo dos nomes dos recursos."
  type        = string
  default     = "AWS-tech-challenge"
}

variable "alert_email" {
  description = "E-mail opcional para receber alertas SNS. Configure localmente em terraform.tfvars. A assinatura precisa ser confirmada."
  type        = string
  default     = ""
}

variable "enable_daily_schedule" {
  description = "Habilita a execução diária automática do job Glue. Deixe false até validar o primeiro run manual."
  type        = bool
  default     = false
}

variable "min_join_match_rate" {
  description = "Taxa mínima de correspondência exigida nos joins bloqueantes."
  type        = number
  default     = 0.95

  validation {
    condition     = var.min_join_match_rate >= 0.0 && var.min_join_match_rate <= 1.0
    error_message = "min_join_match_rate deve estar entre 0 e 1."
  }
}

variable "max_invalid_weight_rate" {
  description = "Fração máxima de alunos presentes com peso ausente/não positivo; esses registros são excluídos do cálculo."
  type        = number
  default     = 0.001

  validation {
    condition     = var.max_invalid_weight_rate >= 0.0 && var.max_invalid_weight_rate <= 1.0
    error_message = "max_invalid_weight_rate deve estar entre 0 e 1."
  }
}
