from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
import os
"""
This is an alternative to creating GCP blocks in the UI
(1) insert your own GCS bucket name
(2) insert your own service_account_file path or service_account_info dictionary from the json file

This assumes you already have service_account_file in json format downloaded in your computer
IMPORTANT - do not store credentials in a publicly available repository..
"""

gcs_bucket_name = "yelp-data-lake-yelp-pipeline-project" # same bucket name as created in terraform
gcs_credentials_block_name = "yelp-gcs-creds"

# Set permanent env var for service_account_file before save: https://phoenixnap.com/kb/set-environment-variable-mac
# Create gcp_credentials block in prefect locally - we are assuming you have service_account_file in json format in your computer
gcp_credentials_block = GcpCredentials(
    service_account_file = os.getenv("GCP_SERVICE_ACCOUNT_FILE")
) #      service_account_info=service_account_info - this also works but is more sensitive to data breach
gcp_credentials_block.save(f"{gcs_credentials_block_name}", overwrite=True)

# load gcp_credentials block 
credentials = GcpCredentials.load(gcs_credentials_block_name)

# Create GcsBucket block in prefect locally
gcs_bucket_block = GcsBucket(
    bucket = gcs_bucket_name,
    gcp_credentials = credentials
)
gcs_bucket_block.save(f"{gcs_bucket_name}", overwrite=True)

# run this file under pwd /Users/davidjeong/Documents/data-engineering-zoomcamp/week_7_project/Yelp_review_datapipeline
# python prefect/prefect_create_blocks.py