from os import listdir
from pathlib import Path
from sqlalchemy import create_engine
import os
os.environ['OPENBLAS_NUM_THREADS'] = '1'
import pandas as pd


class Analyze:

    PRICES_DAYS = "days_price"
    PRICES_HOURS = "hours_price"
    PRICES_MINUTES = "minutes_price"
    RSI_PERIOD = 6

    def execute(self, period: int):
        coins_path = Path(__file__).with_name('coins_data')
        coins_path = str(coins_path.absolute()).replace("services/", "")
        coins_list = listdir(coins_path)
        signal_list = {}
        for coin in coins_list:
            signal = self.analyze(coin, period)
            if signal > 30 or signal == 0:
                continue
            signal_list[signal] = coin
        return sorted(signal_list.items())

    def analyze(self, _code, _period=14):
        engine = create_engine(self.__get_engine(_code), echo=False)
        with engine.connect() as connection:
            daily_price = pd.read_sql_query(
                f"SELECT price from {self.PRICES_DAYS}",
                connection
            )
            rsi = self.calculate_rsi(daily_price, _period)
        return rsi

    def __get_engine(self, _coin_code: str) -> str:
        env_path = Path(__file__).with_name('coins_data')
        env_path = f"sqlite:///{env_path.absolute()}/{_coin_code}".replace("services/", "")
        return env_path

    def calculate_rsi(self, prices: pd.DataFrame, period: int) -> float:
        prices = prices.price.tail(period + 1).astype(float)
        gain = (prices.pct_change()
                .where(prices.diff() > 0, 0.0)
                .tail(period).mean())
        loss = (prices.pct_change()
                .abs()
                .where(prices.diff() < 0, 0.0)
                .tail(period).mean())
        if loss == 0:
            return 100.00
        rs = gain / loss
        return round(100 - (100 / (1 + rs)), 2)
