# Yelp Review Restaurant Recommendations

## Problem Statement
The food and drinks recommendations provided by Google Maps and Yelp are mostly based on specific locations, ratings, and reviews. It is difficult to see if there are any new restaurants to try or holistically assess all of the restaurants in the area. I want to create an ELT pipeline that will enable me to study California's yelp dataset, find places that I have yet tried, and see if I can create a robust restaurant recommendation system competitive as Yelp's and Google's.

## Technology Stack
The following technologies are used to build this project:

- Terraform as Infrastructure-as-Code (IaC) tool to set up Cloud environment
- Yelp Fusion API
- Prefect for orchestration workflow
- Google Cloud Storage (GCS) as Data Lake
- Google BigQuery for Data Warehouse
- dbt for transformation and data modeling
- Google Looker studio for visualizations

## Data Pipeline Architecture
<img width="784" alt="image" src="https://github.com/djeong95/Yelp_review_datapipeline/assets/102641321/54e10af8-57c5-4a24-865d-ccaa4e60ba11">

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
TBD
## Data Insights
TBD
## Future Improvements
- Use Google Compute Engine and Docker to host this data pipeline in the cloud
- Incorporate Yelp Reviews to build some kind of NLP model
- Use MLOps to train/test and deploy the said model
## Reproduce it yourself

1. Fork this repo, and clone it to your local environment.

`git clone https://github.com/djeong95/Yelp_review_datapipeline.git`

2. Setup your Google Cloud environment
- Create a [Google Cloud Platform project](https://console.cloud.google.com/cloud-resource-manager)
- Configure Identity and Access Management (IAM) for the service account, giving it the following privileges:
    - Viewer
    - Storage Admin
    - Storage String (Object) Admin
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
- Type 'yes' when prompted.

5. Setup your orchestration
- If you do not have a prefect workspace, sign-up for the prefect cloud and create a workspace [here](https://app.prefect.cloud/auth/login)
- Create the [prefect blocks](https://docs.prefect.io/2.10.21/concepts/blocks/) via the cloud UI or adjust the variables in /prefect/prefect_create_blocks.py and run
```bash
python prefect/prefect_create_blocks.py
```
- To execute the flow, run the following commands in two different terminals
```bash
prefect agent start -q 'default'
python prefect/yelp_api_to_gcs.py
python prefect/yelp_gcs_to_bq.py
```
After running the flow, you will find the data at BigQuery in yelp_data_raw.{term}_data_raw, the flow will take around 120 mins to complete, but it will vary depending on the term you run. Note that free Yelp API account is limited to 5000 calls each day. 

6. Data tranformation and modeling using dbt

Below is the lineage graph that describes the data transformation performed by dbt. 

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