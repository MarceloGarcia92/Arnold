Request the secret code to have access to the strava info

http://www.strava.com/oauth/authorize?client_id=[YOUR_CLIENT_ID]&response_type=code&redirect_uri=http://localhost/exchange_token&approval_prompt=force&scope=profile:read_all,activity:read_all

Extract the secret code by:

http://localhost/exchange_token?state=&code=[YOUR_CODE]&scope=read,activity:read_all,profile:read_all