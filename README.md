# Yelp_review_datapipeline

## Architecture

Pipeline Breakdown:

### Overview

### ETL Flow

## Data Warehouse

## Data Insights


## Reproduce it yourself

1. Fork this repo, and clone it to your local environment.

`git clone https://github.com/djeong95/Yelp_review_datapipeline.git`

2. Setup your Google Cloud environment
- Create a [Google Cloud Platform project](https://console.cloud.google.com/cloud-resource-manager)
- Configure Identity and Access Management (IAM) for the service account, giving it the following privileges:
    - Viewer
    - Storage Admin
    - Storage Object Admin
    - BigQuery Admin

- Download the JSON credentials and save it, e.g. to `~/.gc/<credentials>`

- Install the [Google Cloud SDK](https://cloud.google.com/sdk/docs/install-sdk)

- Let the [environment variable point to your GCP key](https://cloud.google.com/docs/authentication/application-default-credentials#GAC), authenticate it and refresh the session token
```bash
export GOOGLE_APPLICATION_CREDENTIALS=<path_to_your_credentials>.json
gcloud auth application-default login
```
Check out this [link](https://www.youtube.com/watch?v=Hajwnmj0xfQ&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=12&t=29s&ab_channel=DataTalksClub%E2%AC%9B) for a video walkthrough.

3. Install all required dependencies into your environment
```bash
conda create -n yelp_project python=3.9
conda activate yelp_project
pip install -r requirements.txt
```

4. Setup your infrastructure
Run the following commands to install Terraform - if you are using a different OS please choose the correct version [here](https://developer.hashicorp.com/terraform/tutorials/aws-get-started/install-cli) and exchange the download link and zip file name

```bash
brew tap hashicorp/tap
brew install hashicorp/tap/terraform
# Verify successful install by executing below code:
terraform -help 
```
- Change the variables.tf file with your corresponding variables, I would recommend to leave the names of the datasets and bucket as they are; otherwise you need to change them in the prefect flows and dbt.
- To initiate, plan and apply the infrastructure, adjust and run the following Terraform commands

```bash
cd terraform/
terraform init
terraform plan -var="project=<your-gcp-project-id>"
terraform apply -var="project=<your-gcp-project-id>"
```
Type 'yes' when prompted.

5. Setup your orchestration