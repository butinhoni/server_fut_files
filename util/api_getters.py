import requests
import json
import pandas as pd
from segredos import api_key, api_host

id_test = '499207'

def get_matches(data):
    uri = f'https://api.football-data.org/v4/matches?date={data}'
    headers = { 'X-Auth-Token': 'b1b878a69c1e4caea4ebae08d1d044a8' }

    response = requests.get(uri, headers=headers)
    data = response.json()

    df = pd.json_normalize(data['matches'])
    df.to_csv('temp/matches.csv')
    return df

def get_match_info(id_match):
    uri = f'https://api.football-data.org/v4/matches/{id_match}'
    headers = { 'X-Auth-Token': 'b1b878a69c1e4caea4ebae08d1d044a8' }

    response = requests.get(uri, headers=headers)
    data = response.json()

    print(data)
    df = pd.json_normalize(data)
    print(df)


def get_matchesAPI2():
    uri = 'https://v3.football.api-sports.io/fixtures?date=2025-03-09'
    headers = {'x-rapidapi-key':api_key,
               'x-dapidapi-host':api_host
               }
    
    response = requests.get(uri, headers= headers)
    data = response.json()

    print(data)
    df = pd.json_normalize(data['response'])
    df.to_csv('temp/dataapi2.csv')

get_matchesAPI2()