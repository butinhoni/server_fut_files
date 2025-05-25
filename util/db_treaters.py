import pandas as pd
import database_services

df = pd.read_csv('temp/dataapi2.csv')


colunas_fixture = []
colunas_league = []
for coluna in df.columns:
    if 'fixture' in coluna and 'venue' not in coluna:
        colunas_fixture.append(coluna)
    elif 'id' in coluna:
        colunas_fixture.append(coluna)
    elif 'goals' in coluna or 'score' in coluna:
        colunas_fixture.append(coluna)
    if 'league' in coluna:
        colunas_league.append(coluna)

df_fixtures = df[colunas_fixture]
df_fixtures.to_csv('temp/dbfix.csv')

dfHome = df[['teams.home.id', 'teams.home.name', 'teams.home.logo']]
dfHome.columns = ['id' ,'name', 'logo']
dfAway = df[['teams.away.id', 'teams.away.name', 'teams.away.logo']]
dfAway.columns = ['id' ,'name', 'logo']

df_teams = pd.concat([dfHome, dfAway])

df_leagues = df[colunas_league]
df_leagues.columns = ['id', 'name', 'country', 'logo', 'flag', 'season', 'round', 'standings']


database_services.update_leagues(df_leagues)
database_services.update_teams(df_teams)
database_services.update_fixtures(df_fixtures)





