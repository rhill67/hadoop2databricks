resource "databricks_notebook" "bronze" {
  path     = "/Shared/${var.project_prefix}/01_bronze_ingest"
  source   = "${path.module}/../notebooks/01_bronze_ingest.py"
  language = "PYTHON"
}

resource "databricks_notebook" "silver" {
  path     = "/Shared/${var.project_prefix}/02_silver_transform"
  source   = "${path.module}/../notebooks/02_silver_transform.py"
  language = "PYTHON"
}

resource "databricks_notebook" "gold" {
  path     = "/Shared/${var.project_prefix}/03_gold_marts"
  source   = "${path.module}/../notebooks/03_gold_marts.py"
  language = "PYTHON"
}

resource "databricks_job" "medallion_job" {
  name = "${var.project_prefix}-medallion-job"

  task {
    task_key = "bronze_ingest"
    notebook_task {
      notebook_path = databricks_notebook.bronze.path
    }

    # Option 1: use existing cluster
    dynamic "existing_cluster_id" {
      for_each = var.existing_cluster_id != "" ? [1] : []
      content  = var.existing_cluster_id
    }
  }

  task {
    task_key = "silver_transform"
    depends_on {
      task_key = "bronze_ingest"
    }
    notebook_task {
      notebook_path = databricks_notebook.silver.path
    }

    dynamic "existing_cluster_id" {
      for_each = var.existing_cluster_id != "" ? [1] : []
      content  = var.existing_cluster_id
    }
  }

  task {
    task_key = "gold_marts"
    depends_on {
      task_key = "silver_transform"
    }
    notebook_task {
      notebook_path = databricks_notebook.gold.path
    }

    dynamic "existing_cluster_id" {
      for_each = var.existing_cluster_id != "" ? [1] : []
      content  = var.existing_cluster_id
    }
  }
}
