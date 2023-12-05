import requests
import json 
from fpl_utils import fpl_utils

def lambda_handler(event, context):
    try:
        pl_profile = event['pl_profile']
        cookie = f"pl_profile={pl_profile};"
        
        bucket_name = fpl_utils.get_parameter('fpl_bucket_name')
        gameweek = fpl_utils.get_gameweek()
        team_sheet = fpl_utils.get_json_df(bucket_name, f'gameweek-{gameweek}/full_team.json')

        post_starting_team(event['user_id'], team_sheet, cookie)
    except Exception as e:
        sns_topic = fpl_utils.get_parameter('failure_email_sns_key')
        error_message = f"Error in Lambda function calculate changes: {str(e)}"
        fpl_utils.send_sns_notification(error_message, sns_topic)
        raise e  # Re-raise the exception after sending the notification

def post_starting_team(user_id, team_sheet, cookie):
    headers = {'content-type': 'application/json', 'origin': 'https://fantasy.premierleague.com', 'referer': 'https://fantasy.premierleague.com/my-team', 'cookie' : cookie}
    url = 'https://fantasy.premierleague.com/api/my-team/'+str(user_id) + '/'
    resp = requests.post(url=url, json=json.loads(team_sheet),headers=headers)
    if resp.status_code != 200:
        raise Exception(f'Error: Received status code {resp.status_code} from server: {resp.text}')
