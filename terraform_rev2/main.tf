terraform {
    required_version = ">= 1.0"
    backend "local" {}  # Can change from "local" to "gcs" (for google) or "s3" (for aws),if you would like to preserve your tf-state online
    required_providers {
        google = {
          source  = "hashicorp/google"
        }
    }
}

provider "google" {
    project = var.project
    region = var.region
    credentials = file(var.credentials)  # Use this if you do not want to set env-var GOOGLE_APPLICATION_CREDENTIALS
}

# Data Lake Bucket
# Ref: https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/storage_bucket

resource "google_storage_bucket" "my-data-lake-bucket" {
    name = "${local.data_lake_bucket}-${var.project}-production"
    location = var.region

    # Optional, but recommended settings:
    storage_class = var.storage_class
    uniform_bucket_level_access = true

    versioning {
      enabled = true
    }

    lifecycle_rule {
      action {
        type = "Delete"
      }
      condition {
        age = 30 // days
      }
    }
      
    
    force_destroy = true
}

# DWH
# Ref: https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/bigquery_dataset
resource "google_bigquery_dataset" "yelp_data_raw_prod" {
  dataset_id = var.BQ_DATASET
  project    = var.project
  location   = var.region
}

resource "google_bigquery_dataset" "yelp_data_dbt_prod" {
  dataset_id = var.DBT_DATASET
  project    = var.project
  location   = var.region
}


# # This code is compatible with Terraform 4.25.0 and versions that are backwards compatible to 4.25.0.
# # For information about validating this Terraform code, see https://developer.hashicorp.com/terraform/tutorials/gcp-get-started/google-cloud-platform-build#format-and-validate-the-configuration

# resource "google_compute_instance" "yelp-production" {
#   boot_disk {
#     auto_delete = true
#     device_name = "yelp-production"

#     initialize_params {
#       image = "projects/ubuntu-os-cloud/global/images/ubuntu-2004-focal-v20230812"
#       size  = 40
#       type  = "pd-balanced"
#     }

#     mode = "READ_WRITE"
#   }

#   can_ip_forward      = false
#   deletion_protection = false
#   enable_display      = false

#   labels = {
#     goog-ec-src = "vm_add-tf"
#   }

#   machine_type = "e2-standard-4"
#   name         = "yelp-production"

#   network_interface {
#     access_config {
#       network_tier = "PREMIUM"
#     }

#     subnetwork = "projects/yelp-pipeline-project/regions/us-west2/subnetworks/default"
#   }

#   scheduling {
#     automatic_restart   = true
#     on_host_maintenance = "MIGRATE"
#     preemptible         = false
#     provisioning_model  = "STANDARD"
#   }

#   service_account {
#     email  = "904418004248-compute@developer.gserviceaccount.com"
#     scopes = ["https://www.googleapis.com/auth/devstorage.read_only", "https://www.googleapis.com/auth/logging.write", "https://www.googleapis.com/auth/monitoring.write", "https://www.googleapis.com/auth/service.management.readonly", "https://www.googleapis.com/auth/servicecontrol", "https://www.googleapis.com/auth/trace.append"]
#   }

#   shielded_instance_config {
#     enable_integrity_monitoring = true
#     enable_secure_boot          = false
#     enable_vtpm                 = true
#   }

#   zone = "us-west2-a"
# }


