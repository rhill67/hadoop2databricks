variable "databricks_host" {
  type        = string
  description = "https://dbc-0d2de041-0845.cloud.databricks.com" 
}

variable "databricks_token" {
  type        = string
  description = "Databricks PAT"
  sensitive   = true
}

variable "project_prefix" {
  type        = string
  default     = "phdata-hadoop2databricks"
}

# Choose ONE:
variable "existing_cluster_id" {
  type        = string
  description = "Use an existing cluster ID (recommended for a demo)."
  default     = "063f4a0c7d2bdda2"
}

variable "use_serverless" {
  type        = bool
  description = "If true, run tasks on Serverless (if enabled in your workspace)."
  default     = false
}
