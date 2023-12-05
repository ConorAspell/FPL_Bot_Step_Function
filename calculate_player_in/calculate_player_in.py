import pandas as pd
from fpl_utils import fpl_utils
import os


def lambda_handler(event, context):
    try:
        # Assuming event contains the JSON input structure
        pl_profile = event['pl_profile']
        cookie = f"pl_profile={pl_profile};"
        update_team(event['user_id'], cookie)
        return event
    except Exception as e:
        sns_topic = fpl_utils.get_parameter('failure_email_sns_key')
        error_message = f"Error in Lambda function getPlayerDetails: {str(e)}"
        fpl_utils.send_sns_notification(error_message, sns_topic)
        raise e  # Re-raise the exception after sending the notification
    
    
def update_team(user_id, cookie):
    team = fpl_utils.get_team_auth(user_id, cookie)
    gameweek = fpl_utils.get_gameweek()
    bucket_name = fpl_utils.get_parameter('fpl_bucket_name')
    
    player_out =  fpl_utils.get_json_df(bucket_name, f'gameweek-{gameweek}/player_out.json') 
    players_df = fpl_utils.get_csv_df(bucket_name, f'players-gameweek-{gameweek}.csv')
    
    players = [x['element'] for x in team['picks']]
    
    potential_players = players_df.loc[~players_df.id.isin(players)]
    my_original_team = players_df.loc[players_df.id.isin(players)]
    
    # Get the web_name of the player you want to drop
    player_name = player_out['web_name'].iloc[0]
    
    # Find the index of the player in the original team
    player_index = my_original_team.index[my_original_team['web_name'] == player_name][0]
    
    # Drop the player from the original team
    my_team = my_original_team.drop(player_index)

    bank = team['transfers']['bank']
    budget = player_out.now_cost.iat[0] + bank

    dups_team = my_team.pivot_table(index=['team'], aggfunc='size')
    invalid_teams = dups_team.loc[dups_team==3].index.tolist()

    potential_players=potential_players.loc[~potential_players.team.isin(invalid_teams)]
    potential_players=potential_players.loc[potential_players.element_type==player_out.element_type.iat[0]]
    potential_players = potential_players.loc[potential_players.now_cost<=budget]
    potential_players = calc_in_weights(potential_players)
    
    player_in = potential_players.sample(1, weights=potential_players.in_weight)
    
    my_team = pd.concat([my_team, player_in], ignore_index=True)

    file_name = f'gameweek-{gameweek}/player_in.json'
    fpl_utils.upload_json_to_s3(bucket_name, file_name, player_in.to_dict('records'))

def calc_in_weights(players):
    players['in_weight'] = 1
    players['in_weight'] += players['diff']
    players['in_weight'] += players['form'].astype("float")*10
    players.loc[players['in_weight'] <0, 'in_weight'] =0

    return players.sort_values('in_weight', ascending=False)
