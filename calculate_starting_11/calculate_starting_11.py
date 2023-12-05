import pandas as pd
from fpl_utils import fpl_utils
import os

def lambda_handler(event, context):
    try:
        pl_profile = event['pl_profile']
        cookie = f"pl_profile={pl_profile};"
        update_team(event['user_id'], cookie)
        return event
    except Exception as e:
        sns_topic = fpl_utils.get_parameter('failure_email_sns_key')
        error_message = f"Error in Lambda function calculate changes: {str(e)}"
        fpl_utils.send_sns_notification(error_message, sns_topic)
        raise e  # Re-raise the exception after sending the notification
    
def update_team(user_id, cookie):
    team = fpl_utils.get_team_auth(user_id, cookie)
    gameweek = fpl_utils.get_gameweek()
    bucket_name = fpl_utils.get_parameter('fpl_bucket_name')

    player_out = fpl_utils.get_json_df(bucket_name, f'gameweek-{gameweek}/player_out.json') 
    player_in = fpl_utils.get_json_df(bucket_name, f'gameweek-{gameweek}/player_in.json') 

    players_df = fpl_utils.get_csv_df(bucket_name, f'players-gameweek-{gameweek}.csv')
    players = [x['element'] for x in team['picks']]
    my_original_team = players_df.loc[players_df.id.isin(players)]

    # Get the web_name of the player you want to drop
    player_name = player_out['web_name'].iloc[0]
    
    # Find the index of the player in the original team
    player_index = my_original_team.index[my_original_team['web_name'] == player_name][0]
    
    # Drop the player from the original team
    my_team = my_original_team.drop(player_index)

    my_team = pd.concat([my_team, player_in], ignore_index=True)
    
    my_team = calc_starting_weight(my_team)

    team_sheet = breakdown_team(my_team)

    file_name = f'gameweek-{gameweek}/full_team.json'
    fpl_utils.upload_json_to_s3(bucket_name, file_name, team_sheet)

def calc_starting_weight(players):
    players['starting_weight'] = 1
    players['starting_weight'] += players['diff']/2
    players['starting_weight'] += players['form']*10
    players.loc[players['starting_weight'] <0, 'starting_weight'] =0
    
    return players.sort_values('starting_weight', ascending=False)

    
def breakdown_team(my_team):
    outfied_players = my_team.loc[my_team.element_type>2]

    goalies = my_team.loc[my_team.element_type==1]
    defenders = my_team.loc[my_team.element_type==2]

    captain = outfied_players.id.iat[0]
    vice_captain = outfied_players.id.iat[1]

    starters = pd.concat([goalies.head(1), defenders.head(3), outfied_players.head(7)], ignore_index=True)
    subs = pd.concat([goalies.tail(1), defenders.tail(2), outfied_players.tail(1)], ignore_index=True)

    picks =[]
    count = 1
    for i in range(1,5):
        players = starters.loc[starters.element_type==i]
        ids = players.id.tolist()
        for ide in ids:
            if ide == captain:
                player = {"element" : ide, "is_captain" : True, "is_vice_captain" : False, "position" : count}
            elif ide == vice_captain:
                player = {"element" : ide, "is_captain" : False, "is_vice_captain" : True, "position" : count}
            else:
                player = {"element" : ide, "is_captain" : False, "is_vice_captain" : False, "position" : count}
            picks.append(player.copy())
            count+=1
    ids = subs.id.tolist()
    for ide in ids:
        player = {"element" : ide, "is_captain" : False, "is_vice_captain" : False, "position" : count}
        picks.append(player.copy())
        count+=1
    team_sheet = {"picks" : picks,"chip" : None}
    return team_sheet
