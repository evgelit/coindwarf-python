from binance import Client
import env
from os.path import isfile
from pathlib import Path
from sqlalchemy import create_engine, inspect
from os import environ
environ['OPENBLAS_NUM_THREADS'] = '1'
import pandas as pd
from datetime import datetime


class LoadPrices:

    PRICES_DAYS = "days_price"
    PRICES_HOURS = "hours_price"
    PRICES_MINUTES = "minutes_price"

    def execute(self):
        client = Client(
            env.env['api_key'],
            env.env['api_secret']
        )
        prices = client.get_all_tickers()
        for price in prices:
            if (price['symbol'][-4:] != env.env['base_coin']
                    or self.__is_db_exists(_coin_code=price['symbol'][:-4]) is False):
                continue
            engine = create_engine(
                self.__get_engine(price['symbol'][:-4]),
                echo=False
            )
            with engine.connect() as connection:
                min_price = pd.read_sql_query(
                    f"SELECT * FROM {self.PRICES_MINUTES}",
                    connection
                )
                hour_price = pd.read_sql_query(
                    f"SELECT * FROM {self.PRICES_HOURS}",
                    connection
                )
                day_price = pd.read_sql_query(
                    f"SELECT * FROM {self.PRICES_DAYS}",
                    connection
                )

                min_price.loc[len(min_price.index)] = [
                    str(datetime.now()),
                    price['price']
                ]
                min_price.drop(index=min_price.index[0], axis=0, inplace=True)
                if datetime.now().minute == 0:
                    hour_price.loc[len(hour_price.index)] = [
                        str(datetime.now()),
                        price['price']
                    ]
                    hour_price.drop(index=hour_price.index[0], axis=0, inplace=True)
                else:
                    hour_price.loc[len(hour_price.index) - 1] = [
                        str(datetime.now()),
                        price['price']
                    ]
                if datetime.now().hour == 0 and datetime.now().minute == 0:
                    day_price.loc[len(day_price.index)] = [
                        str(datetime.now()),
                        price['price']
                    ]
                    day_price.drop(index=day_price.index[0], axis=0, inplace=True)
                else:
                    day_price.loc[len(day_price.index) - 1] = [
                        str(datetime.now()),
                        price['price']
                    ]
                min_price.to_sql(
                    name=self.PRICES_MINUTES,
                    con=connection,
                    if_exists='replace',
                    index=False
                )
                hour_price.to_sql(
                    name=self.PRICES_HOURS,
                    con=connection,
                    if_exists='replace',
                    index=False
                )
                day_price.to_sql(
                    name=self.PRICES_DAYS,
                    con=connection,
                    if_exists='replace',
                    index=False
                )
                connection.commit()

    def __get_engine(self, _coin_code: str) -> str:
        env_path = Path(__file__).with_name('coins_data')
        env_path = f"sqlite:///{env_path.absolute()}/{_coin_code}".replace("services/", "")
        return env_path

    def __is_db_exists(self, _coin_code: str) -> bool:
        env_path = Path(__file__).with_name('coins_data')
        env_path = f"{env_path.absolute()}/{_coin_code}".replace("services/", "")
        return isfile(env_path)