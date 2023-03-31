# Introduction 
### Development of two projects:
- Execution Flows: design of an extract, transform and load (ETL) process for different universities.
- Big Data: usage of MapReduce model to process huge amount
of data in parallel, coming from StackOverflow website.

# Installation instructions
### Clone the project repository
    https://github.com/kvc55/Project_Alkemy

### Create a virtualenv
    python -m venv path\to\myenv

### Install the requirements
    pip install -r requirements.txt

### Configure the connection to the database
Create a .ini or .env file configuring the data connection:

    DB_USERNAME = my_user_name
    DB_PASSWORD = my_password
    DB_HOST = host
    DB_PORT = port
    DB_NAME = my_database

### Create and configure the connection to S3 bucket
Create a .ini or .env file configuring the data connection: 

    BUCKET_REGION = selected_region
    KEY_ID = my_bucket_key_id
    ACCESS_KEY = my_bucket_access_key
    bucket_name = my_bucket_name

### Important
Each of the files has the prompt to do the task.