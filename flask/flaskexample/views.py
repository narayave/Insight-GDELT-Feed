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

@app.route('/db_fancy')
def events_page_fancy():
    sql_query = """
               SELECT * FROM gdelt_events;

                """
    query_results=pd.read_sql_query(sql_query,con)

    items = []

    for i in range(0,query_results.shape[0]):
        items.append(dict(globaleventid=query_results.iloc[i]['globaleventid'], sqldate=query_results.iloc[i]['sqldate'], actor1geo_fullname=query_results.iloc[i]['actor1geo_fullname'], actor1name=query_results.iloc[i]['actor1name'])) #, source_url=query_results.iloc[i]['source_url']))
#item['actor1geo_fullname']}}</td><td>{{item['actor1name']}}</td><td>{{item['source_url']}}
    return render_template('test_page.html',items=items)

@app.route('/output')
def location_output():

	#pull 'birth_month' from input field and store it
	loc = request.args.get('location')

	#just select the Cesareans  from the birth dtabase for the month that the user inputs
	query = "SELECT globaleventid, sqldate, actor1name FROM gdelt_events WHERE actor1countrycode='%s'" % loc
	print(query)

	query_results=pd.read_sql_query(query,con)
	print(query_results)
	items = []
	for i in range(0,query_results.shape[0]):
		items.append(dict(eventid=query_results.iloc[i]['globaleventid'], date=query_results.iloc[i]['sqldate'], actor_name=query_results.iloc[i]['actor1name']))
		the_result = ''

	return render_template("output.html", items = items, the_result = the_result)