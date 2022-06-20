terraform {
  required_providers {
    databricks = {
      source = "databrickslabs/databricks"
      version = "0.6.1"
    }
  }
}

provider "databricks" {
  # Configuration options
}