import pandas as pd
import os
import requests
import json
from datetime import datetime

from fpl_utils import fpl_utils

def lambda_handler(event, context):
    try:
        players = fpl_utils.get('https://fantasy.premierleague.com/api/bootstrap-static/')
        players_df = pd.DataFrame(players['elements'])
        teams_df = pd.DataFrame(players['teams'])
        fixtures_df = pd.DataFrame(players['events'])
        today = datetime.now().timestamp()
        fixtures_df = fixtures_df.loc[fixtures_df.deadline_time_epoch>today]
        gameweek =  fpl_utils.get_gameweek() 
        
        teams = dict(zip(teams_df.id, teams_df.name))
        players_df['team_name'] = players_df['team'].map(teams)

        api_key = fpl_utils.get_parameter('odds_data_api_key')
        odds_response = requests.get('https://api.the-odds-api.com/v3/odds/?apiKey='+api_key+'&sport=soccer_epl'+'&region=uk'+'&mkt=h2h')    
        odds_json = json.loads(odds_response.text)
        
        if odds_json['success'] == False:
            api_key = fpl_utils.get_parameter('odds_data_api_second_key')
            odds_response = requests.get('https://api.the-odds-api.com/v3/odds/?apiKey='+api_key+'&sport=soccer_epl'+'&region=uk'+'&mkt=h2h')  
            odds_json = json.loads(odds_response.text)
        sport = {}
        all_sports = []
        this_deadline=fixtures_df.deadline_time_epoch.iat[0]
        next_deadline = fixtures_df.deadline_time_epoch.iat[1]
        if len(fixtures_df.id) == 1:
            next_deadline=fixtures_df.deadline_time_epoch.iat[1] + 5517900

        for item in odds_json["data"]:
            if not (this_deadline < item['commence_time'] < next_deadline):
                continue
            
            sport['id'] =item['id']
            sport['home_team'] = item['home_team']
            sport['away_team'] = [team for team in item['teams'] if team != item['home_team']][0]
            
            sites = [x for x in item['sites'] if x['site_key'] == "paddypower"]
            if len(sites)==0:
                continue
            if item['home_team'] == item['teams'][0]:
                home_odds, away_odds, draw_odds = sites[0]['odds']['h2h']
            else:
                away_odds, home_odds, draw_odds = sites[0]['odds']['h2h']
            sport['home_odds'] = home_odds
            sport['draw_odds'] =  draw_odds   
            sport['away_odds'] = away_odds    
            sport['commence_time'] = item['commence_time']
            sport['home_chance'] = 100/sport['home_odds']
            sport['away_chance'] = 100/sport['away_odds']
            
            all_sports.append(sport.copy())
        df = pd.DataFrame(all_sports)
        bucket_name = fpl_utils.get_parameter('fpl_bucket_name')
        key = "odds-gameweek-" +str(gameweek)+".csv"
        fpl_utils.upload_csv_to_s3(bucket_name, key, df)
        return event
    except Exception as e:
        sns_topic = fpl_utils.get_parameter('failure_email_sns_key')
        error_message = f"Error in Lambda function getPlayerDetails: {str(e)}"
        fpl_utils.send_sns_notification(error_message, sns_topic)
        raise e  # Re-raise the exception after sending the notification
