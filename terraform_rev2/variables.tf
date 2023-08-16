locals {
    data_lake_bucket = "yelp-data-lake"
}

variable "project" {
  description = "Your GCP Project ID"
  default = "yelp-pipeline-project"
  type = string
}

variable "credentials" {
  description = "Your GCP service account credentials"
  default = "/home/dwjeong/yelp-pipeline-project-d37c8a515746.json"
  type = string
}

variable "region" {
  description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
  default = "us-west2"
  type = string
}

variable "storage_class" {
  description = "Storage class for your bucket. Check official docs for more info."
  default = "STANDARD"
}

variable "BQ_DATASET" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  type = string
  default = "yelp_data_raw_prod"
}

variable "DBT_DATASET" {
  description = "BigQuery Dataset that transformed data (from dbt) will be written to and connected to the presentation layer"
  type = string
  default = "yelp_data_dbt_prod"
}