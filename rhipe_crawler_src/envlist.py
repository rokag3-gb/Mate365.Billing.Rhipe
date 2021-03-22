import os

if os.environ['CRAWLER_ENV'] == 'local':
    os.environ['CRAWLER_ENV'] = 'dev'
    os.environ['contractagreement_id'] = ""

    # cliper
    os.environ['cliper_id'] = ""
    os.environ['cliper_pw'] = ""

    # db
    os.environ['DATABASE_TYPE'] = ''
    os.environ['DATABASE_HOST'] = ''
    os.environ['DATABASE_PORT'] = ''
    os.environ['DATABASE_USER'] = ''
    os.environ['DATABASE_PASSWORD'] = ''
    os.environ['DATABASE_NAME'] = ""

    # prism key
    os.environ['client_id'] = ""
    os.environ['client_secret'] = ""

    # s3
    os.environ['s3_access_key'] = ""
    os.environ['s3_secret_key'] = ""
    os.environ['s3_prefix'] = ""
    os.environ['s3_region_name'] = ""
    os.environ['s3_bucket_hosts'] = ''

    os.environ['slack_api_token'] = ""

contractagreement_id = os.environ['CONTRACTAGREEMENT_ID']

# cliper
cliper_id = os.environ['CLIPER_ID']
cliper_pw = os.environ['CLIPER_PW']

# prism key
client_id = os.environ['CLIENT_ID']
client_secret = os.environ['CLIENT_SECRET']

# s3
s3_access_key = os.environ['S3_ACCESS_KEY']
s3_secret_key = os.environ['S3_SECRET_KEY']
s3_prefix = os.environ['S3_PREFIX']
s3_region_name = os.environ['S3_REGION_NAME']
s3_bucket_hosts = os.environ['S3_BUCKET_HOSTS']

# slack_api_token = os.environ['SLACK_API_TOKEN']