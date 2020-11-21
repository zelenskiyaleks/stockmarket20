from __future__ import unicode_literals
import hypercorn.asyncio
import logging
import json
from quart import Quart, render_template_string, request, jsonify
from telethon import TelegramClient, utils
from  quart_cors import cors
import psycopg2
import os


# Telethon client
#client = TelegramClient('SESSION', int(config["Telegram"]['api_id']), config["Telegram"]['api_hash'])
client = TelegramClient('SESSION', int(os.environ['TELEGRAM_API_ID']), os.environ['TELEGRAM_API_HASH'])
#client.parse_mode = 'html'  # <- Render things nicely
client.flood_sleep_threshold = 60 

quart_cfg = hypercorn.Config()
port = int(os.environ.get("PORT", 17995))
quart_cfg.bind = ["0.0.0.0:17995"]
# Quart app
app = Quart(__name__)
app = cors(app, allow_origin="*")
app.secret_key = 'CHANGE THIS TO SOMETHING SECRET'


# Connect the client before we start serving with Quart
@app.before_serving
async def startup():
    await client.connect()


# After we're done serving (near shutdown), clean up the client
@app.after_serving
async def cleanup():
    await client.disconnect()


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


async def main():
    await hypercorn.asyncio.serve(app, quart_cfg)


async def get_news_controller(content):
    command = content['request']
    if command == 'get_news':
        channels = content['body']['channels']
        data, errors = await get_news_model(channels)
        
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


async def get_news_model(channels):
    await client.start()
    print("GET NEWS MODEL") 
    data = {}
    errors = []

    for channel in channels:
        print(channel)
        try: 
            msg = await get_channel(channel)
            data[channel] = msg
        except Exception as e:
            print(e)
            errors.append(f"\nchannel {channel} import error.")
    await client.disconnect()
    return data, errors


async def  get_channel(channel):
    channel_id =await client.get_entity("https://t.me/" + channel)
    msgs = await client.get_messages(channel_id, limit = 1)

    result = []
    for msg in msgs:
        try:
            jsonify(msg.to_dict())
            result.append(msg.to_dict())
        except:
            pass
    return result


def processMessage(msg):
    d = msg.to_dict()
    return d


@app.route('/quotes', methods=['POST'], endpoint='quotes')
async def quotes_route():
    logging.info(request.is_json)
    content = await request.get_json()
    print(content)
    response = await get_quotes_controller(content)
    print(response)
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
            "message": "unknown cpmmand",
        }
    return response 


quotes_query_template = "\
    select t1.DATE, LOW, HIGH, OPEN, CLOSE, VOLUME  from\
    (\
    select DATE, MIN(TIME) as OPEN_TIME, MAX(TIME) as CLOSE_TIME from quotes\
    where TICKER ='%PLACEHOLDER%'\
    group by DATE) t1\
    inner join\
    (select DATE, MIN(LOW) as LOW, MAX(HIGH) as HIGH, sum(VOL) as VOLUME from quotes\
    where TICKER ='%PLACEHOLDER%'\
    group by DATE) t2\
    on t1.DATE = t2.DATE\
    inner join\
    (select DATE, TIME, OPEN from quotes\
    where TICKER ='%PLACEHOLDER%'\
    ) t3\
    on t1.OPEN_TIME = t3.TIME and t1.DATE = t3.DATE\
    inner join\
    (select DATE, TIME, CLOSE from quotes\
    where TICKER ='%PLACEHOLDER%'\
    ) t4\
    on t1.CLOSE_TIME = t4.TIME and t1.DATE = t4.DATE\
    "



user = os.environ['POSTGRES_USER']
password = os.environ['POSTGRES_PASSWORD']
host = os.environ['POSTGRES_HOST']
db = os.environ['POSTGRES_DATABASE']



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
        my_list.append({"date": str(row[0]), "low": float(row[1]), "high": float(row[2]), "open": float(row[3]), "close": float(row[4]), "volume": float(row[5])})

    return my_list   


if __name__ == '__main__':
    client.loop.run_until_complete(main())
