from arnold.parameters import *
from os import getcwd

import requests
import json

class API_request():
    def __init__(self):
        self.client_ID = client_ID
        self.client_secret = client_secret
        self.code = code

    def getting_credentials(self): 
        # Make Strava auth API call with your 
        # client_code, client_secret and code
        response = requests.post(
                            url = 'https://www.strava.com/oauth/token',
                            data = {
                                    'client_id': {client_ID},
                                    'client_secret': {client_secret},
                                    'code': {code},
                                    'grant_type': 'authorization_code'
                                    }
                        )
        
        print('Succesful API request')
        #Save json response as a variable
        strava_tokens = response.json()
        # Save tokens to file
        with open('credentials/strava_tokens.json', 'w') as outfile:
            json.dump(strava_tokens, outfile)

        print(f'Response was saved in {getcwd()}')


    def getting_activity_data(self, url):
        module = url.split('/')[-1]
        # Get the tokens from file to connect to Strava

        print(f'Using credentials')
        with open('credentials/strava_tokens.json') as json_file:
            strava_tokens = json.load(json_file)
        # Loop through all activities
        access_token = strava_tokens['access_token']
        # Get first page of activities from Strava with all fields
        r = requests.get(url + '?access_token=' + access_token)
        r = r.json()
        print(f'Saving {module} data')

        with open(f'{module}/strava_activities.json', 'w') as outfile:
            json.dump(r, outfile)
        