from datetime import datetime
from dateutil.relativedelta import relativedelta
import json
import logging
import bson
import pandas as pd
from aiogram import Bot, Dispatcher, executor, types

API_TOKEN = ''

logging.basicConfig(level=logging.INFO)
bot = Bot(token=API_TOKEN)
dp = Dispatcher(bot)


@dp.message_handler(commands=['start'])
async def send_welcome(message: types.Message):
    await message.reply('Привет!\n'
                        'Этот бот создан по ТЗ компании RLT\n'
                        'Входные данные должны иметь формат\n'
                        '{ \n'
                            '\t"dt_from":"<Дата от (isoformat)>",\n'
                            '\t"dt_upto":"Дата до (isoformat)", \n'
                            '\t"group_type":"Тип агрегации (hour, day, month)" \n'
                        '}')


@dp.message_handler()
async def echo(message: types.Message):
    message_text = json.loads(message.text)
    file = open('sample_collection.bson', 'rb')
    result = {}
    dataset = []
    labels = []
    df = pd.DataFrame(bson.decode_all(file.read()))
    if isinstance(message_text, dict) and 'dt_from' in message_text and 'dt_upto' in message_text and 'group_type' in message_text:
        df_filtered = df[(df['dt'] >= message_text['dt_from']) & (df['dt'] <= message_text['dt_upto'])]
        if message_text['group_type'] == 'month':
            data = {}
            from_dt = datetime.fromisoformat(message_text['dt_from'])
            to_dt = datetime.fromisoformat(message_text['dt_upto'])
            df_grouped = df_filtered.groupby([df_filtered.dt.dt.year, df_filtered.dt.dt.month]).value.sum()
            for row in range(len(df_grouped.index)):
                data[datetime(df_grouped.iloc[[row]].index.tolist()[0][0],
                                       df_grouped.iloc[[row]].index.tolist()[0][1],
                                       1).isoformat()] = int(df_grouped.iloc[row])
            while from_dt <= to_dt:
                if datetime(from_dt.year, from_dt.month, 1).isoformat() in data:
                    dataset.append(data[datetime(from_dt.year, from_dt.month, 1).isoformat()])
                    labels.append(datetime(from_dt.year, from_dt.month, 1).isoformat())
                else:
                    dataset.append(0)
                    labels.append(datetime(from_dt.year, from_dt.month, 1).isoformat())
                from_dt += relativedelta(months=1)
        elif message_text['group_type'] == 'day':
            data = {}
            from_dt = datetime.fromisoformat(message_text['dt_from'])
            to_dt = datetime.fromisoformat(message_text['dt_upto'])
            df_grouped = df_filtered.groupby([df_filtered.dt.dt.year, df_filtered.dt.dt.month, df_filtered.dt.dt.day]).value.sum()
            for row in range(len(df_grouped.index)):
                data[datetime(df_grouped.iloc[[row]].index.tolist()[0][0],
                              df_grouped.iloc[[row]].index.tolist()[0][1],
                              df_grouped.iloc[[row]].index.tolist()[0][2]).isoformat()] = int(df_grouped.iloc[row])
            while from_dt <= to_dt:
                if datetime(from_dt.year, from_dt.month, from_dt.day).isoformat() in data:
                    dataset.append(data[datetime(from_dt.year, from_dt.month, from_dt.day).isoformat()])
                    labels.append(datetime(from_dt.year, from_dt.month, from_dt.day).isoformat())
                else:
                    dataset.append(0)
                    labels.append(datetime(from_dt.year, from_dt.month, from_dt.day).isoformat())
                from_dt += relativedelta(days=1)
        elif message_text['group_type'] == 'hour':
            data = {}
            from_dt = datetime.fromisoformat(message_text['dt_from'])
            to_dt = datetime.fromisoformat(message_text['dt_upto'])
            df_grouped = df_filtered.groupby([df_filtered.dt.dt.year, df_filtered.dt.dt.month, df_filtered.dt.dt.day, df_filtered.dt.dt.hour]).value.sum()
            for row in range(len(df_grouped.index)):
                data[datetime(df_grouped.iloc[[row]].index.tolist()[0][0],
                              df_grouped.iloc[[row]].index.tolist()[0][1],
                              df_grouped.iloc[[row]].index.tolist()[0][2],
                              df_grouped.iloc[[row]].index.tolist()[0][3]).isoformat()] = int(df_grouped.iloc[row])
            while from_dt <= to_dt:
                if datetime(from_dt.year, from_dt.month, from_dt.day, from_dt.hour).isoformat() in data:
                    dataset.append(data[datetime(from_dt.year, from_dt.month, from_dt.day, from_dt.hour).isoformat()])
                    labels.append(datetime(from_dt.year, from_dt.month, from_dt.day, from_dt.hour).isoformat())
                else:
                    dataset.append(0)
                    labels.append(datetime(from_dt.year, from_dt.month, from_dt.day, from_dt.hour).isoformat())
                from_dt += relativedelta(hours=1)
        result['dataset'] = dataset
        result['labels'] = labels
        await message.answer(json.dumps(result))
    else:
        await message.reply('Формат данных не соответствует шаблону')

if __name__ == '__main__':
    executor.start_polling(dp, skip_updates=True)
