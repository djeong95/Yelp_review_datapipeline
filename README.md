# Yelp Review Restaurant Recommendations

<img src="https://github.com/djeong95/Yelp_review_datapipeline/assets/102641321/fa6ca1e8-14fd-4229-ae7b-57dafe5c4dea" width="380">


## Problem Statement
The food and drinks recommendations provided by Google Maps and Yelp are mostly based on specific locations, ratings, and reviews. It is difficult to see if there are any new restaurants to try or holistically assess all of the restaurants in the area. I want to create an ELT pipeline that will enable me to study California's yelp dataset, find places that I have yet tried, and see if I can create a robust restaurant recommendation system competitive as Yelp's and Google's.

## Technology Stack
The following technologies are used to build this project:

- Terraform as Infrastructure-as-Code (IaC) tool to set up Cloud environment
- Yelp Fusion API
- Docker
- Prefect for orchestration workflow
- Google Cloud Storage (GCS) as Data Lake
- Google BigQuery for Data Warehouse
- dbt for transformation and data modeling
- Google Looker studio for visualizations

## Data Pipeline Architecture
<img width="830" alt="image" src="https://github.com/djeong95/Yelp_review_datapipeline/assets/102641321/f909d896-ce46-4f4a-aa4b-df010ffd4391">


## Data Structure
Yelp API file Data Structure:
| Column | Data Type | Description |
| --- | --- | --- |
| id | String  | Unique ID for every restaurant regardless of chain |
| alias | String | Unique "ID" that has name and location and number |
| name | String | Name of restaurant |
| image_url | String | URL for first image that shows up when searched on Yelp  |
| is_closed | Boolean | True for closed; False for open |
| url | String | URL for Yelp  |
| review_count | Integer | Total number of reviews |
| categories | List of Dictionary | List of categories. Example: [{'alias': 'mexican', 'title': 'Mexican'}] |
| rating | Float | Rating out of 5, with 5 being the best |
| coordinates | Dictionary | Coordinate of restaruant. latitude and longitude are keys |
| transactions | List | Example: delivery, pickup, None |
| price | String | '$', '$$', '$$$', '$$$$' depending on price range in the area |
| location | Dictionary | Address in dictionary form |
| phone | String | phone number |
| display_phone | String | phone number |
| distance | Float | distance from location used to search |
    
Final DataDrame Data Structure:
| Column | Data Type | 
| --- | --- |
| alias | String (object) |
| name | String (object) |
| url | String (object)|
| review_count | int64 |
| categories | List (object) |
| ethnic_category | List (object) |
| rating | float64 |
| price | String (object) |
| latitude | float64 |
| longitude | float64 |
| city | String (object) |
| address| String (object) |

## Data Dashboard
Check out the interactive dashboard [here](https://lookerstudio.google.com/s/qQM0NlyHlaI)

![gif2](https://github.com/djeong95/Yelp_review_datapipeline/assets/102641321/ca502f66-9735-429f-8ee4-2874961dec0d)


## Future Improvements
- Use Google Compute Engine and Docker to host this data pipeline in the cloud **(Complete)**
- Incorporate Yelp Reviews to build some kind of NLP model
- Use MLOps to train/test and deploy the said model

## Reproduce it yourself

Prerequisites: Ensure you have Google Cloud Platform, dbt, Prefect Cloud accounts. To run the project, use the following steps:

1. Setup your Google Cloud environment & Generate Yelp API key
- Create a [Google Cloud Platform project](https://console.cloud.google.com/cloud-resource-manager)
- On GCP create a service account with with GCE, GCS and BiqQuery admin privileges. For a walkthrough, click [here](https://www.youtube.com/watch?v=Hajwnmj0xfQ&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=11&ab_channel=DataTalksClub%E2%AC%9B)

- Configure Identity and Access Management (IAM) for the service account, giving it the following privileges:
    - Viewer
    - Storage Admin
    - Storage String (Object) Admin
    - BigQuery Admin
- Generate Yelp API Key by creating an account with [Yelp](https://www.yelp.com/developers/v3/manage_app) and generating a free user API Key (limited to 5000 calls per day)

3. Create a VM (Virtual Machine) with machine type `e2-standard-4` in a region that is the closest to you. Choose `Operating system: Ubuntu` with `Version: Ubuntu 20.04 LTS` and `Size: 40 GB`. For this project, region: `us-west2` (Los Angeles) was chosen.  

4. Set up the VM. For a walkthrough, click [here](https://www.youtube.com/watch?v=ae-CV2KfoN0&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=14&ab_channel=DataTalksClub%E2%AC%9B)
    - After ssh key is generated and the set up is performed similar to the video, you can easily log into VM with config file in .ssh directory. It will prompt you to input your password after this is run.
    ```bash
    # EXECUTE IN DIRECTORY WHERE SSH KEY IS
    touch config 
    code config #open VS code to edit
    ```
    In VS code, edit config file like below:
    ```bash
    Host YOUR_HOST_NAME_HERE 
        HostName YOUR_VM_EXTERNAL_IP_ADDRESS_HERE
        User YOUR_VM_USERNAME_HERE
        IdentifyFile ~/YOUR_GCP_SSH_KEY_FILE_PATH_HERE
    ```
    Log into your GCP VM by executing `ssh YOUR_HOST_NAME_HERE` in a directory one above `/.ssh/` and typing the passphrase for that ssh.
    - After logging into the VM, install Anaconda using the following steps:
        - Download anaconda using `wget https://repo.anaconda.com/archive/Anaconda3-2023.07-2-Linux-x86_64.sh` or the latest version from this [link](https://www.anaconda.com/download#downloads).
        - Run `bash Anaconda3-2023.07-2-Linux-x86_64.sh` to execute the download file. Accept all the terms and conditions of download.
        - Then run the code below in your VM CLI.
        ```bash
        rm Anaconda3-2023.07-2-Linux-x86_64.sh
        echo 'export PATH="~/anaconda/bin:$PATH"' >> ~/.bashrc 
        
        # Reload default profile
        source ~/.bashrc

        conda update conda
        ```

        - Install Docker and create a user & Install terraform by using the following steps:
            ```bash
            sudo apt-get update # if sudo not installed, do apt-get -y install sudo and then do sudo apt-get update
            sudo apt-get install nano
            sudo apt-get install docker.io
            sudo groupadd docker
            sudo gpasswd -a $USER docker
            sudo docker service restart
            ```
4. Logout by `logout` and logging back in with `ssh YOUR_HOST_NAME_HERE`, assuming `config` file is set up. This is so that your group membership is re-evaluated. If not, you can run `ssh -i ~/YOUR_GCP_SSH_KEY_FILE_PATH_HERE YOUR_VM_USERNAME_HERE@YOUR_VM_EXTERNAL_IP_ADDRESS_HERE`.

5. Fork this repo, and clone it to your local environment.

`git clone https://github.com/djeong95/Yelp_review_datapipeline.git`

6. Inside the VM under `/home/YOUR_USERNAME/Yelp_review_pipeline/` directory, create both `.env` and `SERVICE_ACCT_KEY.json` file.
```bash
touch .env
nano .env

touch SERVICE_ACCT_KEY.json
nano SERVICE_ACCT_KEY.json
```

- Inside the `.env` file, paste below information.
    ```bash
    YELP_API_KEY=YOUR_YELP_API_KEY_HERE
    GCP_SERVICE_ACCOUNT_FILE=YOUR_SERVICE_ACCOUNT_KEY_JSONFILEPATH_HERE
    GOOGLE_CLOUD_PROJECT=YOUR_PROJECT_NAME_HERE
    ```
- Inside the `SERVICE_ACCT_KEY.json` file, paste below information from your service account key that you generated.
    ```bash
    {
  "type": "service_account",
  "project_id": "YOUR_PROJECT_ID_HERE",
  "private_key_id": "YOUR_PRIVATE_KEY_ID_HERE",
  "private_key": "YOUR_PRIVATE_KEY_HERE",
  "client_email": "YOUR_CLIENT_EMAIL_HERE",
  "client_id": "YOUR_CLIENT_ID_HERE",
  "auth_uri": "YOUR_INFORMATION_HERE",
  "token_uri": "YOUR_INFORMATION_HERE",
  "auth_provider_x509_cert_url": "YOUR_INFORMATION_HERE",
  "client_x509_cert_url": "YOUR_INFORMATION_HERE",
  "universe_domain": "YOUR_DOMAIN_HERE"
    }
    ```
6. In your VM, you are now ready to build a Docker container from an image, which is defined using a Dockerfile.
```bash
# build docker image
docker build -t yelp-pipeline-production:latest .

# After docker container is built, add volumes for .env and api_key.json files
docker run -it -v /home/YOUR_USERNAME/Yelp_review_datapipeline/.env:/opt/prefect/.env -v /home/YOUR_USERNAME/Yelp_review_datapipeline/SERVICE_ACCT_KEY.json:/opt/prefect/SERVICE_ACCT_KEY.json yelp-pipeline-production
```

7. Install required tools and download & install Terraform.
```bash
cd /usr/local/bin/
TERRAFORM_VERSION="1.5.5"
apt-get update
apt-get install -y curl unzip
curl -LO "https://releases.hashicorp.com/terraform/${TERRAFORM_VERSION}/terraform_${TERRAFORM_VERSION}_linux_amd64.zip"
unzip terraform_${TERRAFORM_VERSION}_linux_amd64.zip
rm terraform_${TERRAFORM_VERSION}_linux_amd64.zip
```
8. Run Terraform to create infrastructure.
```bash
cd /opt/terraform_rev2/
```

- If needed, use `nano variables.tf` to change the `variables.tf` file with your corresponding variables (`apt-get install nano` might be needed). I would recommend to leave the names of the datasets and bucket as they are; otherwise you need to change them in the prefect flows and dbt.
- To initiate, plan and apply the infrastructure, adjust and run the following Terraform commands.

```bash
terraform init
terraform plan -var="project=YOUR_PROJECT_ID_HERE"
terraform apply -var="project=YOUR_PROJECT_ID_HERE"
```
- Type 'yes' when prompted.

9. Setup your orchestration
- If you do not have a prefect workspace, sign-up for the prefect cloud and create a workspace [here](https://app.prefect.cloud/auth/login).
- Generate `PREFECT_API_KEY` and `PREFECT_API_URL` for login via Docker container. 
- Create the [prefect blocks](https://docs.prefect.io/2.10.21/concepts/blocks/) via the cloud UI or adjust the variables in /prefect/prefect_create_blocks.py and run.
```bash
cd /opt/prefect/
mkdir data/
PREFECT_API_KEY="[API-KEY]"
PREFECT_API_URL="https://api.prefect.cloud/api/accounts/[ACCOUNT-ID]/workspaces/[WORKSPACE-ID]"
prefect cloud login
python prefect/prefect_create_blocks.py
```
- Run `prefect deployment build` commands for deploying scheduled runs. 
```bash
cd /opt/
prefect deployment build ./prefect/yelp_api_to_gcs.py:etl_api_to_gcs -n "Parameterized ETL from API to GCS"
prefect deployment build ./prefect/yelp_gcs_to_bq.py:etl_gcs_to_bq -n "Parameterized ETL from GCS datalake to BigQuery"
```
- Edit parameters as you see fit in the `etl_api_to_gcs-deployment.yaml` and `etl_gcs_to_bq-deployment.yaml` files generated by using `nano etl_api_to_gcs-deployment.yaml`.

    **example:** 
    `parameters: {"terms": ['Restaurants'], "start_slice": 0, "end_slice": 459}`
- To execute the flow, run the following commands in two different terminals
```bash
prefect deployment apply etl_api_to_gcs-deployment.yaml
prefect deployment apply etl_gcs_to_bq-deployment.yaml
prefect agent start --work-queue "default" # run this to schedule your run
```
After running the flow, you will find the data at BigQuery in yelp_data_raw.{term}_data_raw, the flow will take around 120 mins to complete, but it will vary depending on the term you run. Note that free Yelp API account is limited to 5000 calls each day. 

10. Data tranformation and modeling using dbt
- Follow the steps highlighted [here](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/week_4_analytics_engineering/dbt_cloud_setup.md) to create an account and create a dbt cloud project. Skip the Create a BigQuery service account, as you already have a service account key.

- Below is the lineage graph that describes the data transformation performed by dbt. 

**Lineage Graph**
<img width="1079" alt="image" src="https://github.com/djeong95/Yelp_review_datapipeline/assets/102641321/78023bda-f2cf-4f04-9731-d6853caf76e2">

- Execute run the following command:

```dbt build --var 'is_test_run: false'```

You will get 6 tables in the yelp_data_dbt database:

- dim_coordinates
- dim_counties
- fact_yelp_all
- stg_cafes
- stg_desserts
- stg_restaurants

## Data Vizualization and Dashboarding
- You can now query the data and connect it to looker to visualize the data, when connecting to data source use fact_yelp_all table to build the data source in looker, don't use partitioned table, as you wont get any data in your report.
- Go to [Looker Studio](https://lookerstudio.google.com/) → create → BigQuery → choose your project, dataset & transformed table.