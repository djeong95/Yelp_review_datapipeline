import os
from dotenv import load_dotenv
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
from prefect.infrastructure.docker import DockerContainer
from prefect.deployments import Deployment
from yelp_api_to_gcs import main

"""
This is an alternative to creating GCP blocks in the UI
(1) insert your own GCS bucket name
(2) insert your own service_account_file path or service_account_info dictionary from the json file

This assumes you already have service_account_file in json format downloaded in your computer
IMPORTANT - do not store credentials in a publicly available repository..
"""
load_dotenv()

gcs_bucket_name = "yelp-data-lake-yelp-pipeline-project-production" # same bucket name as created in terraform
gcs_credentials_block_name = "yelp-gcs-creds"
docker_block_name = "yelp-pipeline-container-production"

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


# docker_block = DockerContainer(
#     image="yelp-pipeline-container-production",
#     image_pull_policy="ALWAYS"
# )
# docker_block.save(f"{docker_block_name}", overwrite=True)
# docker_container_block = DockerContainer.load(docker_block_name)
# print(docker_container_block)

# docker_dep = Deployment.build_from_flow(
#     flow = main,
#     name = "docker-yelp-api-to-gcs",
#     infrastructure = docker_container_block
# ) 

# if __name__ =='__main__':
#     docker_dep.apply()
# run this file under pwd /Users/davidjeong/Documents/data-engineering-zoomcamp/week_7_project/Yelp_review_datapipeline
# python prefect/prefect_create_blocks.py