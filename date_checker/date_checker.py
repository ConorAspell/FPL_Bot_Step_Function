from datetime import datetime, timedelta
import requests
import json
import pandas as pd

from fpl_utils import fpl_utils

def lambda_handler(event, context):
    today = datetime.now()
    players =  requests.get('https://fantasy.premierleague.com/api/bootstrap-static/')
    players = json.loads(players.content)
    fixtures_df = pd.DataFrame(players['events'])
    
    tomorrow = (today + timedelta(days=1)).timestamp()
    today = datetime.now().timestamp()
    fixtures_df = fixtures_df.loc[fixtures_df.deadline_time_epoch > today]
    deadline = fixtures_df.iloc[0].deadline_time_epoch
    
    if deadline < tomorrow:
        return True
    else:
        seconds_diff = deadline - today
        
        days, remainder = divmod(seconds_diff, 86400)  # 86400 seconds in a day
        hours, remainder = divmod(remainder, 3600)  # 3600 seconds in an hour
        minutes = remainder // 60  # remaining seconds to minutes
        
        formatted_time = '{} days, {} hours, {} minutes'.format(int(days), int(hours), int(minutes))
        sns_topic = fpl_utils.get_parameter('failure_email_sns_key')
        fpl_utils.send_sns_notification('Deadline is not approaching, there are {} until the next deadline'.format(formatted_time), sns_topic)
        return False
