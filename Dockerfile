FROM prefecthq/prefect:2.10.4-python3.9
FROM hashicorp/terraform:latest

RUN apt-get update
RUN apt-get install -y wget

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
