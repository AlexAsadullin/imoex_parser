import asyncio
import json
import pandas as pd
import re
import pyrogram
import datetime as dt
from config import api_id, api_hash


def remove_emoji(text):
    regrex_pattern = re.compile(pattern="["
                                        u"\U00000000-\U00000009"
                                        u"\U0000000B-\U0000001F"
                                        u"\U00000080-\U00000400"
                                        u"\U00000402-\U0000040F"
                                        u"\U00000450-\U00000450"
                                        u"\U00000452-\U0010FFFF"
                                        "]+", flags=re.UNICODE)
    return regrex_pattern.sub(r'', text)


async def read_tg_channel(sourse_channel_id: int, lookback=7):
    client = pyrogram.Client(name='client1', api_id=api_id, api_hash=api_hash)
    await client.start()
    try:
        history = []
        actual_time_unix = dt.datetime.now().timestamp()
        end = actual_time_unix - lookback * 24 * 60 * 60
        messages = client.get_chat_history(chat_id=sourse_channel_id, limit=100)
        async for i in messages:
            if i.date.timestamp() < end:
                break
            elif i.text:
                history.append((i.date.strftime('%Y-%m-%d'), i.text))
        print('info added successfully')
    finally:
        await client.stop()
        return history


if __name__ == '__main__':
    KEYWORDS = ('актив', 'акци', 'анали', 'валют', 'волантильн', 'инвест', 'инструмент', 'компани', 'кризис', 'крипто',
                'ликвидн', 'олигаци', 'пассив', 'риск', 'рынок', 'рыноч', 'сигнал', 'срок', 'стратег', 'тенденц',
                'торг', 'трейд', 'тренд', 'фонд')

    channels = {
        'topotLive': -1001754252633,
        'РБК': -1001099860397,
        'ТинькоффИнвест': -1001344086554,
        'КодДурова': -1001043793945,
        'РБК_Инвест': -1001498653424,
        'ФинансоваяНезависимость': -1001189883642,
        'Хулиномика': -1001380524958,
        'ПростаяЭкономика': -1001903548131,
        'InvestingCom': -1001369248634,
    }
    economical_news = []

    for i in channels.keys():
        # lookback = number of DAYS we read news in channel
        history = asyncio.run(read_tg_channel(channels[i], lookback=10))
        for date, text in history:
            for k in KEYWORDS:
                if k in text:
                    economical_news.append(remove_emoji(text))
                    break
        print(i, 'sucsess')
    with open('news.json', 'w', encoding='utf-8') as f:
        f.write(json.dumps(economical_news, ensure_ascii=False))
    with open('news.txt', 'a') as f:
        for line in economical_news:
            f.write(line)
            f.write('\n')
    print('data successfully saved')
