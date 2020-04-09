import asyncio
import csv
import os
from datetime import datetime

import aiohttp
from aiohttp import web
import requests
import motor.motor_asyncio as aiomotor


TOKEN = os.environ.get("API_TOKEN")
API_BASE_URL = "https://partners-api.999.md"


async def fetch_resource(session, url):
    async with session.get(url) as response:
        response.raise_for_status()
        return await response.json()


async def handler_categories(request):
    auth = aiohttp.BasicAuth(login=TOKEN, password="", encoding="utf-8")

    async with aiohttp.ClientSession(auth=auth) as session:
        categories = await fetch_resource(session, f"{API_BASE_URL}/categories?lang=ro")

    db_response = await request.app["db"].categories.insert_many(
        categories["categories"]
    )
    print(f"Inserted {len(db_response.inserted_ids)}")
    return web.Response(text=f"Fetched {len(db_response.inserted_ids)} categories")


async def handler_adverts(request):
    auth = aiohttp.BasicAuth(login=TOKEN, password="", encoding="utf-8")

    async with aiohttp.ClientSession(auth=auth) as session:
        response = await fetch_resource(session, f"{API_BASE_URL}/adverts?lang=ro")

        tasks = [
            asyncio.ensure_future(
                fetch_resource(session, f"{API_BASE_URL}/adverts/{adv['id']}?lang=ro")
            )
            for adv in response["adverts"]
        ]
        adverts = await asyncio.gather(*tasks)

    # Here, before storing each advert, the conversion of EUR -> MDL would be done
    # From what I see, the API is not very consistent or I don't understand it
    # And some keys don't match the docs
    # But, the computation would be done here using the exchange rate from app['eur_rate']
    # Then store them in mon

    db_response = await request.app["db"].adverts.insert_many(adverts)
    print(f"Inserted {len(db_response.inserted_ids)}")
    return web.Response(text=f"Fetched {len(db_response.inserted_ids)} adverts")


async def init_mongo(loop):
    url = f"mongodb://{os.environ.get('MONGO_HOST', 'localhost')}:{os.environ.get('MONGO_PORT', 27017)}"
    conn = aiomotor.AsyncIOMotorClient(url, maxPoolSize=2, io_loop=loop)
    db = os.environ.get('DB_NAME')
    return conn[db]


async def setup_mongo(app, loop):
    db = await init_mongo(loop)

    async def close_mongo(app):
        db.client.close()

    app.on_cleanup.append(close_mongo)
    return db


def get_exchange_rate():
    exchange_url = f'https://bnm.md/ro/export-official-exchange-rates?date={datetime.now().date().strftime("%d.%m.%Y")}'
    response = requests.get(exchange_url)
    iterator = (x.decode("utf-8") for x in response.iter_lines(decode_unicode=True))
    reader = csv.reader(iterator, delimiter=";")
    usd_row = next(filter(lambda x: x[0] == "Euro", reader), None)
    return float(usd_row[-1].replace(",", ".")) if usd_row else None


async def make_app():
    exchange_rate = get_exchange_rate()
    app = web.Application()
    loop = asyncio.get_event_loop()

    db = await setup_mongo(app, loop)
    app["db"] = db
    app["eur_rate"] = exchange_rate

    app.router.add_get("/categories", handler_categories)
    app.router.add_get("/adverts", handler_adverts)
    return app


if __name__ == "__main__":
    web.run_app(make_app(), port=8000)
