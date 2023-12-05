import requests
import json 
import pandas as pd
from datetime import datetime
import boto3
import io
import boto3

def get_parameter(parameter):
    ssm = boto3.client('ssm', 'eu-west-1')
    return ssm.get_parameter(Name=parameter, WithDecryption=True)['Parameter']['Value']


def send_sns_notification(message, topic_arn):
    sns = boto3.client('sns')  # replace with your SNS Topic ARN
    response = sns.publish(
        TopicArn=topic_arn,
        Message=message,
    )

def get_csv_df(bucket, key):
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=bucket, Key=key)
    df = pd.read_csv(io.BytesIO(obj['Body'].read()))
    return df
    
    
def get_json_df(bucket, key):
    s3 = boto3.client('s3')
    # Get the file from S3
    obj = s3.get_object(Bucket=bucket, Key=key)
    data = obj['Body'].read().decode('utf-8')
    
    # Convert the JSON data to a pandas DataFrame
    df = pd.read_json(io.StringIO(data))
    return df

def upload_json_to_s3(bucket_name, file_name, data):
    """
    Uploads the given data to the specified S3 bucket and file name.
    """
    s3 = boto3.resource('s3')
    obj = s3.Object(bucket_name, file_name)
    obj.put(Body=json.dumps(data))

def upload_csv_to_s3(bucket_name, file_name, df):
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    s3_resource = boto3.resource('s3')
    s3_resource.Object(bucket_name, file_name).put(Body=csv_buffer.getvalue())
    
def get(url):
    response = requests.get(url)
    return json.loads(response.content)

def get_gameweek():
    players =  get('https://fantasy.premierleague.com/api/bootstrap-static/')
    fixtures_df = pd.DataFrame(players['events'])
    today = datetime.now().timestamp()
    fixtures_df = fixtures_df.loc[fixtures_df.deadline_time_epoch>today]
    gameweek =  fixtures_df.id.iat[0]
    return gameweek

def get_team_auth(user_id, cookie):
    url = f'https://fantasy.premierleague.com/api/my-team/{user_id}/'
    headers = {
        'cookie': cookie,
    }

    resp = requests.get(url, headers=headers)
    if resp.status_code != 200:
        raise Exception(f'Error: Received status code {resp.status_code} from server: {resp.text}')
    team = json.loads(resp.content)
    return team
