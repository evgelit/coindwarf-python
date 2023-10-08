from .resource import Resource
from .entity import Entity as Coin
from os import path
from pathlib import Path


class Repository:

    def __init__(self):
        self.__resource = Resource()

    def load(self, _coin_code: str, _with_prices=False) -> Coin:
        if not path.isfile(self.get_db_path(_coin_code)):
            return Coin("")
        coin = Coin(_coin_code)
        coin = self.__resource.load_config(coin)
        if _with_prices:
            coin = self.__resource.load_prices(coin)
        return coin

    def load_prices(self, _coin: Coin) -> Coin:
        return self.__resource.load_prices(_coin)

    def add_price(self, _coin: Coin, _price: float) -> Coin:
        return self.__resource.add_price(_coin=_coin, _price=_price)

    def setup(self, _coin_code: str, _data: dict):
        self.__resource.setup(_code=_coin_code, _data=_data)

    def move_to_archive(self, _coin: Coin) -> None:
        self.__resource.move_to_archive(_coin=_coin)

    def get_db_path(self, _coin_code: str) -> str:
        env_path = Path(__file__).with_name('data')
        env_path = f"{env_path.absolute()}/{_coin_code}".replace("coins/", "")
        return env_path
