from __future__ import unicode_literals
import hypercorn.asyncio
import logging
import json
from quart import Quart, render_template_string, request, jsonify
from telethon import TelegramClient, utils
from  quart_cors import cors
import psycopg2
import os


quart_cfg = hypercorn.Config()
port = int(os.environ.get("PORT", 17995))
quart_cfg.bind = ["0.0.0.0:" + str(port)]
# Quart app
app = Quart(__name__)
app = cors(app, allow_origin="*")
app.secret_key = 'CHANGE THIS TO SOMETHING SECRET'
# db connection
user = os.environ['POSTGRES_USER']
password = os.environ['POSTGRES_PASSWORD']
host = os.environ['POSTGRES_HOST']
db = os.environ['POSTGRES_DATABASE']


@app.route('/news', methods=['POST'], endpoint='news')
async def news_route():
    logging.info(request.is_json)
    content = await request.get_json()
    print(content)
    response = await get_news_controller(content)
    logging.info(f"response: {response}")
    return jsonify(response)


@app.route('/', methods=['GET'], endpoint='nmain')
async def root_route():
    return "Hello"


async def get_news_controller(content):
    command = content['request']
    if command == 'get_news':
        channels = content['body']['channels']
        keywords = content['body']['keywords']
        data, errors = await get_news_model(channels, keywords)
        
        if len(data) == 0:
            response = {
                "state": "error",
                "message": "data not found",
            }
        else:        
            response = {
            "state": "OK",
            "response": data,
            "errors": errors 
        }
    else:
        response = {
            "state": "error",
            "message": "unknown command",
        }
    return response 


async def get_news_model(channels, keywords):
    errors = []
    msg = await get_channel(channels, keywords)
    return msg, errors


async def  get_channel(channels, keywords):
    print("get_channels", channels, keywords)
    channel_querry = list(map(lambda x: "'" + str(x) + "'", channels))
    where_clause_channel = "channel in (" + ','.join(channel_querry) + ") and "

    keywords_clause = list(map(lambda x: "or lower(message) like '%" + str(x).lower() + "%'", keywords))
    where_clause_keywords = '(False ' + ' '.join(keywords_clause) + ')'

    query = 'select * from telegram where ' + where_clause_channel + where_clause_keywords + ' order by timestamp desc LIMIT 30'

    conn = psycopg2.connect( 
        user=user, 
        password=password,
        host=host, 
        dbname=db)

    cursor = conn.cursor()
    my_list = []

    cursor = conn.cursor()
    cursor.execute(query)
    result = cursor.fetchall()

    for row in result:
        my_list.append({"channel": str(row[0]), "id": int(row[1]), "message": str(row[2]), "timestamp": str(row[3])})

    return my_list


@app.route('/quotes', methods=['POST'], endpoint='quotes')
async def quotes_route():
    logging.info(request.is_json)
    content = await request.get_json()
    response = await get_quotes_controller(content)
    logging.info(f"response: {response}")
    return jsonify(response)


async def get_quotes_controller(content):
    command = content['request']

    if command == 'get_quotes':
        ticker = content['body']['ticker']
        data = get_quotes_model(ticker)
        
        if len(data) == 0:
            response = {
                "state": "error",
                "message": "data not found",
            }
        else:        
            response = {
            "state": "OK",
            "response": data 
        }
    else:
        response = {
            "state": "error",
            "message": "unknown command",
        }
    return response 


quotes_query_template = "\
    select t1.DATE, LOW, HIGH, OPEN, CLOSE, VOLUME  from\
    (\
    select timestamp::date as DATE, MIN(timestamp::time) as OPEN_TIME, MAX(timestamp::time) as CLOSE_TIME from allquotes\
    where TICKER ='%PLACEHOLDER%'\
    group by DATE) t1\
    inner join\
    (select timestamp::date as DATE, MIN(LOW) as LOW, MAX(HIGH) as HIGH, sum(VOL) as VOLUME from allquotes\
    where TICKER ='%PLACEHOLDER%'\
    group by DATE) t2\
    on t1.DATE = t2.DATE\
    inner join\
    (select timestamp::date as DATE, timestamp::time as TIME, OPEN from allquotes\
    where TICKER ='%PLACEHOLDER%'\
    ) t3\
    on t1.OPEN_TIME = t3.TIME and t1.DATE = t3.DATE\
    inner join\
    (select timestamp::date as DATE, timestamp::time as TIME, CLOSE from allquotes\
    where TICKER ='%PLACEHOLDER%'\
    ) t4\
    on t1.CLOSE_TIME = t4.TIME and t1.DATE = t4.DATE order by t1.DATE asc\
    "

def get_quotes_model(ticker):
    conn = psycopg2.connect( 
        user=user, 
        password=password,
        host=host, 
        dbname=db)

    cursor = conn.cursor()

    query = quotes_query_template.replace('%PLACEHOLDER%', ticker)
    my_list = []

    cursor = conn.cursor()
    cursor.execute(query)
    result = cursor.fetchall()

    for row in result:
        my_list.append({
            "date": str(row[0]), 
            "low": float(row[1]), 
            "high": float(row[2]), 
            "open": float(row[3]), 
            "close": float(row[4]), 
            "volume": float(row[5])})

    return my_list   


@app.route('/forecasts', methods=['POST'], endpoint='forecasts')
async def forecasts_route():
    logging.info(request.is_json)
    content = await request.get_json()
    response = await get_forecasts_controller(content)
    logging.info(f"response: {response}")
    return jsonify(response)


async def get_forecasts_controller(content):
    command = content['request']

    if command == 'get_forecasts':
        ticker = content['body']['ticker']
        data = get_forecasts_model(ticker)
        
        if len(data) == 0:
            response = {
                "state": "error",
                "message": "data not found",
            }
        else:        
            response = {
            "state": "OK",
            "response": data 
        }
    else:
        response = {
            "state": "error",
            "message": "unknown command",
        }
    return response 


forecasts_query_template = "select ticker, forecastdate, forecasteddate, forecastedprice, comment from forecasts where ticker = '%PLACEHOLDER%'"

def get_forecasts_model(ticker):
    conn = psycopg2.connect( 
        user=user, 
        password=password,
        host=host, 
        dbname=db)

    cursor = conn.cursor()

    query = forecasts_query_template.replace('%PLACEHOLDER%', ticker)
    my_list = []

    cursor = conn.cursor()
    cursor.execute(query)
    result = cursor.fetchall()

    for row in result:
        my_list.append({
            "ticker": str(row[0]), 
            "forecastdate": str(row[1]), 
            "forecasteddate": str(row[2]), 
            "forecastedprice": float(row[3]), 
            "comment": str(row[4])})

    return my_list   


app.run(host='0.0.0.0', port=int(os.environ.get("PORT", 17995))) 