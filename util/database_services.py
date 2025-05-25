import psycopg2
from segredos import dbname, user, password, host, port
from sqlalchemy import create_engine

def get_db_connection():
    conn = psycopg2.connect(
        database = dbname,
        user = user,
        password = password,
        host = host,
        port = port
    )

    return conn


def criaTabela(tabela, nome_tabela):

    db_config = {
    'dbname': dbname,
    'user': user,
    'password': password,
    'host': host,
    'port': port
    }

    # Cria uma string de conexão para o SQLAlchemy
    connection_string = f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}"

    # Cria uma engine de conexão
    engine = create_engine(connection_string)



    # Envia o DataFrame para o PostgreSQL
    tabela.to_sql(nome_tabela, engine, if_exists='append', index=False)

    print(f"Tabela '{nome_tabela}' enviada com sucesso para o PostgreSQL!")

def update_teams(df):

    conn = get_db_connection()
    cur = conn.cursor()

    for ind, item in df.iterrows():
        cur.execute(
            '''
            INSERT INTO public.teams (
            "id",
            "name",
            logo )
            VALUES (%s, %s, %s)
            ON CONFLICT (id) DO UPDATE
            SET
                "name" = EXCLUDED."name",
                "logo" = EXCLUDED."logo"
            ''', (item['id'], item['name'], item['logo'])
            
        )
    conn.commit()
    cur.close()
    conn.close()
    print('Times Atualizados')

def update_fixtures(df):
    conn = get_db_connection()
    cur = conn.cursor()

    for ind, item in df.iterrows():
        cur.execute(
            '''
            INSERT INTO public.fixtures (
            "fixture.id",
            "fixture.referee",
            "fixture.timezone",
            "fixture.date",
            "fixture.timestamp",
            "fixture.periods.first",
            "fixture.periods.second",
            "fixture.venue.id",
            "fixture.status.long",
            "fixture.status.short",
            "league.id",
            "teams.home.id",
            "teams.away.id",
            "goals.home",
            "goals.away",
            "score.halftime.home",
            "score.halftime.away",
            "score.fulltime.home",
            "score.fulltime.away",
            "score.extratime.home",
            "score.extratime.away",
            "score.penalty.home",
            "score.penalty.away")
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT ("fixture.id") DO UPDATE
            SET
                "fixture.referee" = EXCLUDED."fixture.referee",
                "fixture.timezone" = EXCLUDED."fixture.timezone",
                "fixture.date" = EXCLUDED."fixture.date",
                "fixture.timestamp" = EXCLUDED."fixture.timestamp",
                "fixture.periods.first" = EXCLUDED."fixture.periods.first",
                "fixture.periods.second" = EXCLUDED."fixture.periods.second",
                "fixture.venue.id" = EXCLUDED."fixture.venue.id",
                "fixture.status.long" = EXCLUDED."fixture.status.long",
                "fixture.status.short" = EXCLUDED."fixture.status.short",
                "league.id" = EXCLUDED."league.id",
                "teams.home.id" = EXCLUDED."teams.home.id",
                "teams.away.id" = EXCLUDED."teams.away.id",
                "goals.home" = EXCLUDED."goals.home",
                "goals.away" = EXCLUDED."goals.away",
                "score.halftime.home" = EXCLUDED."score.halftime.home",
                "score.halftime.away" = EXCLUDED."score.halftime.away",
                "score.fulltime.home" = EXCLUDED."score.fulltime.home",
                "score.fulltime.away" = EXCLUDED."score.fulltime.away",
                "score.extratime.home" = EXCLUDED."score.extratime.home",
                "score.extratime.away" = EXCLUDED."score.extratime.away",
                "score.penalty.home" = EXCLUDED."score.penalty.home",
                "score.penalty.away" = EXCLUDED."score.penalty.away"
            ''', (
                item['fixture.id'],
                item['fixture.referee'],
                item['fixture.timezone'],
                item['fixture.date'],
                item['fixture.timestamp'],
                item['fixture.periods.first'],
                item['fixture.periods.second'],
                item['fixture.venue.id'],
                item['fixture.status.long'],
                item['fixture.status.short'],
                item['league.id'],
                item['teams.home.id'],
                item['teams.away.id'],
                item['goals.home'],
                item['goals.away'],
                item['score.halftime.home'],
                item['score.halftime.away'],
                item['score.fulltime.home'],
                item['score.fulltime.away'],
                item['score.extratime.home'],
                item['score.extratime.away'],
                item['score.penalty.home'],
                item['score.penalty.away']
            )
        )
    conn.commit()
    cur.close()
    conn.close()
    print('Partidas Atualizadas')


def update_leagues(df):
    conn = get_db_connection()
    cur = conn.cursor()

    for ind, item in df.iterrows():
        cur.execute(
            '''
            INSERT INTO public.leagues (
            "id",
            "name",
            country,
            logo,
            flag,
            season,
            round,
            standings)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT ("id") DO UPDATE
            SET
                "name" = EXCLUDED."name",
                "country" = EXCLUDED."country",
                "logo" = EXCLUDED."logo",
                "flag" = EXCLUDED."flag",
                "season" = EXCLUDED."season",
                "round" = EXCLUDED."round",
                "standings" = EXCLUDED."standings"
            ''', (
                item['id'],
                item['name'],
                item['country'],
                item['logo'],
                item['flag'],
                item['season'],
                item['round'],
                item['standings']
            )
        )
    
    conn.commit()
    cur.close()
    conn.close()
    print('Ligas Atualizadas')