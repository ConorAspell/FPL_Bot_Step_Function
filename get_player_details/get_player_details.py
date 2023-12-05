import requests
import pandas as pd
import boto3
from fpl_utils import fpl_utils
import os

def lambda_handler(event, context):
    try:
        bucket_name = fpl_utils.get_parameter('fpl_bucket_name')
        players_df, fixtures_df, gameweek = get_data(bucket_name)
        players_df['gameweek'] = gameweek
        players_df = players_df[columns]
        fpl_utils.upload_csv_to_s3(bucket_name, f"players-gameweek-{gameweek}.csv",players_df)
        fpl_utils.upload_csv_to_s3(bucket_name, f"game-gameweek-{gameweek}.csv", players_df)
        return event
    except Exception as e:
        sns_topic = fpl_utils.get_parameter('failure_email_sns_key')
        error_message = f"Error in Lambda function getPlayerDetails: {str(e)}"
        fpl_utils.send_sns_notification(error_message, sns_topic)
        raise e  # Re-raise the exception after sending the notification

necessary_columns = ['element_type', 'id', 'now_cost', 'team','web_name', 'diff', 'in_weight', 'out_weight','gameweek']

columns = ['chance_of_playing_next_round', 'chance_of_playing_this_round',
 'element_type', 'ep_next',
       'ep_this',  'first_name', 'form', 'id', 'in_dreamteam',
        'now_cost', 'points_per_game',
       'second_name', 'selected_by_percent', 
        'team', 'team_code', 'total_points', 'transfers_in',
        'transfers_out',
       'value_form', 'value_season', 'web_name',      
        'influence', 'creativity', 'threat',
       'ict_index', 'diff']

def get_data(bucket_name):
    players =  fpl_utils.get('https://fantasy.premierleague.com/api/bootstrap-static/')
    players_df = pd.DataFrame(players['elements'])
    teams_df = pd.DataFrame(players['teams'])
    fixtures_df = pd.DataFrame(players['events'])

    gameweek =  fpl_utils.get_gameweek()
    
    key = "odds-gameweek-" +str(gameweek) +".csv"

    bet_df = fpl_utils.get_csv_df(bucket_name, key)

    players_df.chance_of_playing_next_round = players_df.chance_of_playing_next_round.fillna(100.0)
    players_df.chance_of_playing_this_round = players_df.chance_of_playing_this_round.fillna(100.0)
    fixtures = fpl_utils.get('https://fantasy.premierleague.com/api/fixtures/?event='+str(gameweek))
    fixtures_df = pd.DataFrame(fixtures)

    fixtures_df = fixtures_df.drop(columns=['id'])
    teams = dict(zip(teams_df.id, teams_df.name))
    players_df['team_name'] = players_df['team'].map(teams)
    fixtures_df['team_a_name'] = fixtures_df['team_a'].map(teams)
    fixtures_df['team_h_name'] = fixtures_df['team_h'].map(teams)

    bet_df = bet_df.sort_values('home_team').reset_index()
    fixtures_df = fixtures_df.sort_values('team_h').reset_index()

    fixtures_df['home_chance'] = bet_df['home_chance']
    fixtures_df['away_chance'] = bet_df['away_chance']

    a_players = pd.merge(players_df, fixtures_df, how="inner", left_on=["team"], right_on=["team_a"])
    h_players = pd.merge(players_df, fixtures_df, how="inner", left_on=["team"], right_on=["team_h"])

    a_players['diff'] = a_players['away_chance'] - a_players['home_chance']
    h_players['diff'] = h_players['home_chance'] - h_players['away_chance']

    players_df = a_players.append(h_players)
    return players_df, fixtures_df, gameweek
