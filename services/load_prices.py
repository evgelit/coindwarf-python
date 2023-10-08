from binance import Client
from coins.repository import Repository
import env
from datetime import datetime

class LoadPrices:

    def execute(self):
        client = Client(
            env.env['api_key'],
            env.env['api_secret']
        )
        prices = client.get_all_tickers()
        repository = Repository()
        for price in prices:
            if price['symbol'][-4:] != env.env['base_coin']:
                continue
            coin = repository.load(_coin_code=price['symbol'][:-4])
            if coin.code == "":
                continue
            if 'next_archive' not in coin.config:
                coin.config['next_archive'] = 0

            if float(coin.config['next_archive']) < datetime.now().timestamp():
                coin = repository.load(_coin_code=price['symbol'][:-4], _with_prices=True)
                repository.move_to_archive(_coin=coin)
            repository.add_price(
                _coin=coin,
                _price=float(price['price'])
            )
