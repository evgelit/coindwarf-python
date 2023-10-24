from pathlib import Path
from binance import Client
from sqlalchemy import create_engine, inspect
from os import environ
environ['OPENBLAS_NUM_THREADS'] = '1'
from pandas import DataFrame, to_datetime
import env
from progress.bar import IncrementalBar

class Setup:

    CONFIG_TABLE = "config"
    PRICES_DAYS = "days_price"
    PRICES_HOURS = "hours_price"
    PRICES_MINUTES = "minutes_price"

    def __init__(self):

        self.client = Client(
            env.env['api_key'],
            env.env['api_secret']
        )

    def execute(self):
        prices = self.client.get_exchange_info()
        print("Installing coins data")
        bar = IncrementalBar('Installing coins data', max=len(prices['symbols']))
        for asset in prices['symbols']:
            bar.message = asset['baseAsset']
            asset_data = {
                'config': [['status', '1']],
                'price_data': None
            }
            if asset['status'] != 'TRADING' \
                    or asset['quoteAsset'] != env.env['base_coin']:
                bar.next()
                continue
            for _filter in asset['filters']:
                if _filter['filterType'] == "PRICE_FILTER":
                    asset_data['config'].append(['min_price', _filter['minPrice']])
                if _filter['filterType'] == "LOT_SIZE":
                    asset_data['config'].append(['min_qty', _filter['minQty']])
            asset_data['price_data'] = self.__prepare_price_data(
                symbol=asset['symbol']
            )
            asset_data['config'] = DataFrame(asset_data['config'])
            asset_data['config'].columns = ("key", "value")
            self.__setup(_code=asset['baseAsset'], _data=asset_data)
            bar.next()
        bar.finish()
        print("Done")

    def __prepare_price_data(self, symbol: str):
        klines_days = self.client.get_historical_klines(
            symbol,
            Client.KLINE_INTERVAL_1DAY,
            "15 days"
        )
        klines_hours = self.client.get_historical_klines(
            symbol,
            Client.KLINE_INTERVAL_1HOUR,
            "15 hours"
        )
        klines_minutes = self.client.get_historical_klines(
            symbol,
            Client.KLINE_INTERVAL_1MINUTE,
            "15 minutes"
        )
        return {
            "days": self.__prepare_frame(klines_days),
            "hours": self.__prepare_frame(klines_hours),
            "minutes": self.__prepare_frame(klines_minutes),
        }

    def __prepare_frame(self, klines: list) -> DataFrame:
        price_frame = DataFrame(klines)
        price_frame[0] = to_datetime(price_frame[0], unit='ms')
        price_frame = price_frame.filter([0, 4], axis=1)
        price_frame.columns = ("date", "price")
        return price_frame

    def __get_engine(self, _coin_code: str) -> str:
        env_path = Path(__file__).with_name('coins_data')
        env_path = f"sqlite:///{env_path.absolute()}/{_coin_code}".replace("services/", "")
        return env_path

    def __setup(self, _code: str, _data: dict) -> None:
        engine = create_engine(self.__get_engine(_code), echo=False)
        if inspect(engine).has_table(self.CONFIG_TABLE):
            return
        with engine.connect() as connection:
            _data['config'].to_sql(
                name=self.CONFIG_TABLE,
                con=connection,
                if_exists='replace',
                index=False
            )
            _data['price_data']['days'].to_sql(
                name=self.PRICES_DAYS,
                con=connection,
                if_exists='replace',
                index=False
            )
            _data['price_data']['hours'].to_sql(
                name=self.PRICES_HOURS,
                con=connection,
                if_exists='replace',
                index=False
            )
            _data['price_data']['minutes'].to_sql(
                name=self.PRICES_MINUTES,
                con=connection,
                if_exists='replace',
                index=False
            )
            connection.commit()
