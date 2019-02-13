from flask import render_template
from flaskexample import app
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
import pandas as pd
import psycopg2
from flask import request
from six.moves import configparser

import json
import plotly
import pandasql as ps

config = configparser.ConfigParser()
# TODO: Make sure to read the correct config.ini file on AWS workers
config.read('/home/vee/repos/Insight-GDELT-Feed/flask/flaskexample/config.ini')
dbname = config.get('dbauth', 'dbname')
dbuser = config.get('dbauth', 'user')
dbpass = config.get('dbauth', 'password')
dbhost = config.get('dbauth', 'host')
dbport = config.get('dbauth', 'port')

db = create_engine('postgres://%s%s/%s'%(dbuser,dbhost,dbname))
con = None
con = psycopg2.connect(database = dbname, host = dbhost, user = dbuser, password = dbpass)

@app.route('/')
@app.route('/index')
def index():
    return render_template("input.html", #"index.html",
       title = 'Home', user = { 'nickname': 'Vee' },
       )


@app.route('/db')
def events_page():
    sql_query = """
                SELECT * FROM events;
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
               SELECT * FROM events;

                """
    query_results=pd.read_sql_query(sql_query,con)

    items = []

    for i in range(0,query_results.shape[0]):
        items.append(dict(globaleventid=query_results.iloc[i]['globaleventid'], \
            sqldate=query_results.iloc[i]['sqldate'], \
            actor1geo_fullname=query_results.iloc[i]['actor1geo_fullname'], \
            actor1name=query_results.iloc[i]['actor1name']))
            #, source_url=query_results.iloc[i]['source_url']))
            # item['actor1geo_fullname']}}</td><td>{{item['actor1name']}}</td><td>{{item['source_url']}}
    return render_template('test_page.html',items=items)


@app.route('/results')
def home_page_results():

    all_actor_roles = ["COP", "GOV", "JUD", "BUS", "CRM", "DEV", "EDU", "ENV" \
                            "HLH", "LEG", "MED", "MNC"]

    loc = request.args.get('location')
    checks = request.args.getlist('check_list[]')
    checks = [i.encode('utf-8') for i in checks]
    ticks = checks
    print checks


    if len(checks) == 1:
        query = "SELECT * FROM central_results WHERE action_state='%s' and \
                    Actor1Type1Code = '%s' ORDER BY year DESC;" %(loc, checks[0])
    elif len(checks) > 1:
        # query = "SELECT * FROM central_results WHERE action_state='%s' and \
        #     'Actor1Type1Code' IN %s ORDER BY \"Year\" DESC;" %(loc, tuple(checks))
        query = "SELECT * FROM central_results WHERE action_state='%s' and \
            actor_type IN %s ORDER BY year DESC;" %(loc, tuple(checks))
    elif checks == []:
        query = "SELECT * FROM central_results WHERE action_state='%s' ORDER BY year DESC;" %(loc)

    print query

    # query_results=pd.read_sql_query("SELECT * FROM central_results;",con)
    query_results=pd.read_sql_query(query,con)
    print query_results
    data = []
    # for i in range(0,query_results.shape[0]):
    #     items.append (dict(state=query_results.iloc[i]['action_state'], \
    #                     year=query_results.iloc[i]['year'], \
    #                     actortype=query_results.iloc[i]['actor_type'], \
    #                     count=query_results.iloc[i]['event_count'], \
    #                     goldsteinscale=query_results.iloc[i]['goldstein_scale'], \
    #                     avgtone=query_results.iloc[i]['avg_tone']))

    for i in ticks:
        query = "SELECT year, actor_type, events_count, norm_scale FROM query_results WHERE actor_type ='"+i+"'"

        results_tmp = ps.sqldf(query, locals())
        print results_tmp

        event_counts = results_tmp['events_count'].values
        norms_scale = results_tmp['norm_scale'].values

        scores_tmp = [i / j for i, j in zip(event_counts, norms_scale)]

        print 'Scores_tmp ' + str(scores_tmp)


        years = map(int, list(results_tmp['year'].values))
        scores = map(float, scores_tmp)
        # for item in query
        data.append(dict(x=years, y=scores, name=results_tmp['actor_type'][0], type='line'))

    print data

    print ps.sqldf(query, locals())

    # years = map(int, list(query_results['year'].values))
    # print years
    # scales = map(float, list(query_results['goldstein_scale'].values))
    # print scales

    graphs = [
            dict( #data,
                data=[
                    dict(
                        x=years,
                        y=scores,
                        name='original',
                        type='line'
                    ),
                    dict(
                        x=[2005, 2007, 2012, 2018],
                        y=[2.0, -2, 4, 1],
                        name='test',
                        type='line'
                    )
                ],
                layout=dict(
                    title='first graph'
                )
            )
    ]

    # Add "ids" to each of the graphs to pass up to the client
        # for templating
    ids = ['graph-{}'.format(i) for i, _ in enumerate(graphs)]

    # Convert the figures to JSON
    # PlotlyJSONEncoder appropriately converts pandas, datetime, etc
    # objects to their JSON equivalents
    graphJSON = json.dumps(graphs, cls=plotly.utils.PlotlyJSONEncoder)

    print graphJSON

    # return render_template("results.html", items = items)
    return render_template("results.html", ids=ids, graphJSON=graphJSON)


@app.route('/output')
def location_output():

    #pull 'birth_month' from input field and store it
    loc = request.args.get('location')
    checks = request.args.getlist('check_list[]')
    print checks

    #just select the Cesareans  from the birth dtabase for the month that the user inputs
    query = "SELECT globaleventid, sqldate, actor1name, \
            sourceurl FROM events WHERE actor1geo_countrycode='%s' ORDER BY sqLdate DESC LIMIT 10" %(loc)
    print(query)

    query_results=pd.read_sql_query(query,con)
    print(query_results)
    items = []
    for i in range(0,query_results.shape[0]):
        items.append(dict(eventid=query_results.iloc[i]['globaleventid'], \
                        date=query_results.iloc[i]['sqldate'], \
                        actor_name=query_results.iloc[i]['actor1name'], \
                        sourceurl=query_results.iloc[i]['sourceurl']))

        #the_result = ''

    return render_template("output.html", items = items) #, the_result = the_result)
