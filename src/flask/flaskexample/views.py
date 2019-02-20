from flask import render_template
from flask import request

import json
import pandas as pd
import pandasql as ps
import plotly
from pprint import pprint
import psycopg2
from six.moves import configparser
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists
from sqlalchemy_utils import create_database

from flaskexample import app


config = configparser.ConfigParser()
# NOTE: Make sure to read the correct config.ini file on AWS workers
config.read('/home/ubuntu/Insight-GDELT-Feed/flask/flaskexample/config.ini')
dbname = config.get('dbauth', 'dbname')
dbuser = config.get('dbauth', 'user')
dbpass = config.get('dbauth', 'password')
dbhost = config.get('dbauth', 'host')
dbport = config.get('dbauth', 'port')

db = create_engine('postgres://%s%s/%s' % (dbuser, dbhost, dbname))
con = None
con = psycopg2.connect(database=dbname, host=dbhost,
                       user=dbuser, password=dbpass)


@app.route('/')
@app.route('/index')
def index():
    return render_template("input.html",
                           title='Home', user={'nickname': 'Vee'},
                           )


@app.route('/results')
def home_page_results():

    all_actor_roles = ["COP", "GOV", "JUD", "BUS", "CRM", "DEV", "EDU", "ENV",
                       "HLH", "LEG", "MED", "MNC"]

    loc = request.args.get('location').upper()
    checks = request.args.getlist('check_list[]')
    checks = [i.encode('utf-8') for i in checks]
    ticks = checks
    print loc
    print checks

    if len(checks) == 1:
        query = "SELECT * FROM monthyr_central_results WHERE action_state='%s' \
                    and actor_type = '%s' and CAST(year AS INTEGER) >= 2010 \
                    ORDER BY month_year;" % (loc, checks[0])
    elif len(checks) > 1:
        query = "SELECT * FROM monthyr_central_results WHERE action_state='%s' \
                    and actor_type IN %s and CAST(year as INTEGER) >= 2013 \
                    ORDER BY month_year;" % (loc, tuple(checks))
    elif checks == []:
        query = "SELECT * FROM monthyr_central_results WHERE action_state='%s' \
                    and CAST(year as INTEGER) >= 2013 ORDER BY month_year \
                    DESC;" % (loc)

    print query

    query_results = pd.read_sql_query(query, con)
    print query_results

    results_dict = []
    overall_result = []
    for i in ticks:
        query = "SELECT year, month_year, actor_type, events_count, norm_scale \
                    FROM query_results WHERE actor_type ='"+i+"'"

        results_tmp = ps.sqldf(query, locals())
        print results_tmp

        event_counts = results_tmp['events_count'].values
        norms_scale = results_tmp['norm_scale'].values

        scores_avg = [(j / i)*100 for i, j in zip(event_counts, norms_scale)]
        print 'Scores_avg ' + str(scores_avg)

        years = map(int, list(results_tmp['month_year'].values))
        scores = map(float, scores_avg)

        # Add logic to check last 2 months
	if len(scores_avg) != 0 and len(scores_avg) > 2:
		last_month = scores[-2]
		this_month = scores[-1]
		verdict = 1 if this_month >= last_month else 0
		overall_result.append(
			dict(actor=results_tmp['actor_type'][0], verdict=verdict)
		)
		print last_month, this_month, verdict

        try:
            # for item in query
            results_dict.append(
                dict(x=years, y=scores, name=results_tmp['actor_type'][0],
                     type='line'))
        except Exception as e:
            print 'Error message - ' + str(e)
            continue

    pprint(results_dict)

    print ps.sqldf(query, locals())

    graphs = [
        dict(  # data,
            data=[item for item in results_dict],
            layout=dict(
                title='Result',
                xaxis=dict(title='Years', type='category'),
                yaxis=dict(title='Impact score (%)')
            )
        )
    ]

    # Add "ids" to each of the graphs to pass up to the client for templating
    ids = ['Results:']

    # Convert the figures to JSON
    # PlotlyJSONEncoder appropriately converts pandas, datetime, etc
    # objects to their JSON equivalents
    graphJSON = json.dumps(graphs, cls=plotly.utils.PlotlyJSONEncoder)

    print graphJSON

    return render_template("results.html", ids=ids, graphJSON=graphJSON,
                           overall=overall_result)


@app.route('/output')
def location_output():

    # pull 'birth_month' from input field and store it
    loc = request.args.get('location')
    checks = request.args.getlist('check_list[]')
    print checks

    query = "SELECT globaleventid, sqldate, actor1name, \
            sourceurl FROM events WHERE actor1geo_countrycode='%s' ORDER BY \
            sqLdate DESC LIMIT 10" % (loc)
    print(query)

    query_results = pd.read_sql_query(query, con)
    print(query_results)
    items = []
    for i in range(0, query_results.shape[0]):
        items.append(dict(eventid=query_results.iloc[i]['globaleventid'],
                          date=query_results.iloc[i]['sqldate'],
                          actor_name=query_results.iloc[i]['actor1name'],
                          sourceurl=query_results.iloc[i]['sourceurl']))

        #the_result = ''

    # , the_result = the_result)
    return render_template("output.html", items=items)
