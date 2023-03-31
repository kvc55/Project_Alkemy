from decouple import config

# Configuration to access database
DB_USERNAME = config('DB_USERNAME')
DB_PASSWORD = config('DB_PASSWORD')
DB_HOST = config('DB_HOST')
DB_PORT = config('DB_PORT')
DB_NAME = config('DB_NAME')

DB_CONNECTION = f'postgresql+psycopg2://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'

# Configuration to access S3 bucket on AWS
BUCKET_REGION = config('BUCKET_REGION')
KEY_ID = config('KEY_ID')
ACCESS_KEY = config('ACCESS_KEY')
bucket_name = config('bucket_name')
