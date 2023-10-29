import json
import logging
import os
from datetime import datetime, timedelta

import pymongo
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import (ApplicationBuilder, CommandHandler, ContextTypes,
                          MessageHandler, filters)

load_dotenv()

TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
DB_COLLECTION_NAME = os.getenv('DB_COLLECTION_NAME')
MONTHS_IN_YEAR = 12
SECONDS_IN_HOUR = 3600
MONTH = 'month'
DAY = 'day'
HOUR = 'hour'
ERROR_MESSAGE = 'Error occured, try to send a valid json'

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    filename='logs.log'
)

client = pymongo.MongoClient(DB_HOST, int(DB_PORT))
db = client[DB_NAME]
collection = db.get_collection(DB_COLLECTION_NAME)


def get_data_from_db(dt_from: datetime, dt_upto: datetime):
    dt_filter = {
        'dt': {
            '$gte': dt_from,
            '$lte': dt_upto
        }
    }
    return collection.find(dt_filter).sort('dt', pymongo.ASCENDING)


def aggregate_data(dt_from: str, dt_upto: str, group_type: str):
    dt_from: datetime = datetime.fromisoformat(dt_from)
    dt_upto: datetime = datetime.fromisoformat(dt_upto)

    items = get_data_from_db(dt_from, dt_upto)

    res = {
        'dataset': [],
        'labels': []
    }

    current_date = dt_from
    while current_date <= dt_upto:
        res['labels'].append(datetime.isoformat(current_date))
        res['dataset'].append(0)
        if group_type == MONTH:
            current_date += relativedelta(months=1)
        if group_type == DAY:
            current_date += timedelta(days=1)
        if group_type == HOUR:
            current_date += timedelta(hours=1)
    res['labels'].append('9999-09-01T00:00:00')

    res_index = 0
    for item in items:
        date: datetime = item['dt']
        value: int = item['value']
        start_period = datetime.fromisoformat(res['labels'][res_index])
        end_period = datetime.fromisoformat(res['labels'][res_index + 1])
        while not (start_period <= date < end_period):
            res_index += 1
            start_period = datetime.fromisoformat(res['labels'][res_index])
            end_period = datetime.fromisoformat(res['labels'][res_index + 1])
        res['dataset'][res_index] += value
    res['labels'].pop()
    return res


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text="Please, send a json query"
    )


async def get_data_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        query = json.loads(update.message.text)
        aggregated_data = aggregate_data(
            query['dt_from'],
            query['dt_upto'],
            query['group_type']
        )
        response = json.dumps(aggregated_data)
    except Exception as e:
        response = ERROR_MESSAGE
        logging.error(e)

    await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text=response
    )


def main():
    application = ApplicationBuilder().token(TELEGRAM_TOKEN).build()

    start_handler = CommandHandler('start', start)
    message_handler = MessageHandler(
        filters.TEXT & (~filters.COMMAND),
        get_data_handler
    )

    application.add_handler(start_handler)
    application.add_handler(message_handler)

    application.run_polling()


if __name__ == '__main__':
    main()
