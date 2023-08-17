# Stage 1: Use the Ubuntu image to install wget and other potential utilities
FROM ubuntu:20.04

RUN apt-get update
RUN apt-get install -y wget

# Set the Terraform version as an argument for reusability
ARG TERRAFORM_VERSION=1.5.5

# Install required tools and download & install Terraform
RUN apt-get update && \
    apt-get install -y curl unzip && \
    curl -LO "https://releases.hashicorp.com/terraform/${TERRAFORM_VERSION}/terraform_${TERRAFORM_VERSION}_linux_amd64.zip" && \
    unzip "terraform_${TERRAFORM_VERSION}_linux_amd64.zip" -d /usr/local/bin && \
    rm "terraform_${TERRAFORM_VERSION}_linux_amd64.zip"

# Stage 2: Build the main image based on Prefect with tools from stage 1
FROM prefecthq/prefect:2.10.4-python3.9
COPY requirements.txt ./
RUN pip install -r requirements.txt

# copy terraform files
COPY terraform_rev2/main.tf /opt/terraform_rev2/main.tf
COPY terraform_rev2/variables.tf /opt/terraform_rev2/variables.tf

# copy prefect py files
COPY prefect/yelp_api_to_gcs.py /opt/prefect/yelp_api_to_gcs.py
COPY prefect/yelp_gcs_to_bq.py /opt/prefect/yelp_gcs_to_bq.py
COPY prefect/prefect_create_blocks.py /opt/prefect/prefect_create_blocks.py

# copy reference files
COPY california_county_cities.csv /usr/local/share/california_county_cities.csv
COPY california_lat_long_cities.csv /usr/local/share/california_lat_long_cities.csv
#   - './ny_taxi_postgres_data:/var/lib/postgresql/data:rw'
