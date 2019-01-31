from flask import render_template
from flaskexample import app
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
import pandas as pd
import psycopg2


user = 'gres' #add your username here (same as previous postgreSQL)                      
host = 'localhost'
dbname = 'gdelt'

db = create_engine('postgres://%s%s/%s'%(user,host,dbname))
con = None
con = psycopg2.connect(database = dbname, user = user)

@app.route('/')
@app.route('/index')
def index():
    return render_template("index.html",
       title = 'Home', user = { 'nickname': 'Vee' },
       )

@app.route('/db')
def events_page():
    sql_query = """
                SELECT * FROM gdelt_events;          
                """

    query_results = pd.read_sql_query(sql_query,con)
    events = ""
    for i in range(0,10):
        events += (query_results.iloc[i]['actor1geo_fullname'])
        events += "<br>"
    return events
