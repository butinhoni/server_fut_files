from flask import Flask, request, jsonify, make_response
from flask_jwt_extended import(
    JWTManager, create_access_token, create_refresh_token,
    jwt_required, get_jwt_identity, set_access_cookies, set_refresh_cookies,
    unset_jwt_cookies
)
import psycopg2
from util import segredos
from psycopg2.extras import RealDictCursor
from psycopg2 import sql
from dotenv import load_dotenv
import os
import pandas as pd

#merged

load_dotenv('.env')

app = Flask(__name__)

app.config['JWT_SECRET_KEY'] = os.getenv('JWT_SECRET_KEY') #chave para assinar os tokens
app.config['JWT_TOKEN_LOCATION'] = ['cookies'] #armazenar tokens em cookies
app.config['JWT_COOKIE_CSRF_PROTECT'] = False #proteger contra csrf - ver se arrumo isso depois
app.config['JWT_COOKIE_SECURE'] = False #depois do certbot mudar para TRUE
app.config['JWT_ACCESS_TOKEN_EXPIRES'] = 900 # token de acesso
app.config['JWT_REFRESH_TOKEN_EXPIRES'] = 604800 #7 dias para o token de refresh

jwt = JWTManager(app)

def get_db_connection():
    conn = psycopg2.connect(
        database = segredos.dbname,
        password = segredos.password,
        host = segredos.host,
        port = segredos.port,
        user = segredos.user
    )
    return conn


@app.route('/get_matches', methods = ['GET'])
def get_people():
    try:
        conn = get_db_connection()

        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(''' SELECT 
                            p."fixture.id" AS fixture_id,
                            p."goals.home" AS goals_home,
                            p."goals.away" AS goals_away,
                            p."fixture.status.long" AS status_long,
                            p."fixture.date" AS "date",
                            home_team."name" AS home_team_name,
                            away_team."name" AS away_team_name,
                            league."name" AS league_name
                        FROM 
                            public.fixtures p
                        JOIN 
                            public.teams home_team ON p."teams.home.id" = home_team.id
                        JOIN 
                            public.teams away_team ON p."teams.away.id" = away_team.id
                        JOIN 
                            public.leagues league ON p."league.id" = league.id;

                        ''')
        result = cur.fetchall()

        cur.close()
        conn.close()

        return jsonify(result), 200
    except Exception as e:
        return jsonify({'error':str(e)}), 500