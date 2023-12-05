import requests
import json 

from fpl_utils import fpl_utils

def lambda_handler(event, context):
    try:
        user_id = event['user_id']
        pl_profile = event['pl_profile']
        
        # Concatenate the individual parts into one cookie string
        cookie = f"pl_profile={pl_profile};"
        bucket_name = fpl_utils.get_parameter('fpl_bucket_name')
        gameweek = fpl_utils.get_gameweek()

        player_in = fpl_utils.get_json_df(bucket_name, f'gameweek-{gameweek}/player_in.json')
        player_out = fpl_utils.get_json_df(bucket_name, f'gameweek-{gameweek}/player_out.json') 

        post_change(player_in, player_out, gameweek, user_id, cookie)
        return event
    except Exception as e:
        sns_topic = fpl_utils.get_parameter('failure_email_sns_key')
        error_message = f"Error in Lambda function calculate changes: {str(e)}"
        fpl_utils.send_sns_notification(error_message, sns_topic)
        raise e  # Re-raise the exception after sending the notifica

def post_change(player_in, player_out, gameweek, user_id, cookie):

    print(player_in.head())
    headers = {'content-type': 'application/json', 'origin': 'https://fantasy.premierleague.com', 'referer': 'https://fantasy.premierleague.com/transfers', 'cookie' : cookie}
    transfers = [{"element_in" : int(player_in.id.iat[0]), "element_out" : int(player_out.id.iat[0]),"purchase_price": int(player_in.now_cost.iat[0]), "selling_price" : int(player_out.now_cost.iat[0])}]
    transfer_payload = { "transfers" : transfers,"chip" : None,"entry" : user_id,"event" : int(gameweek)}
    url = 'https://fantasy.premierleague.com/api/transfers/'

    print("Transferring Out: " + player_out.web_name.iat[0] + ", Transferring In: " + player_in.web_name.iat[0])

    resp = requests.post(url=url, json=transfer_payload, headers=headers)
    if resp.status_code != 200:
        raise Exception(f'Error: Received status code {resp.status_code} from server: {resp.text}')
    return 'success'
