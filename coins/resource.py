from sqlalchemy import (create_engine,
                        inspect,
                        MetaData,
                        Float,
                        String,
                        Integer,
                        Table,
                        Column,
                        text
                        )
from .entity import Entity as Coin
from time import time
from pathlib import Path
from datetime import datetime


class Resource:
    CONFIG_TABLE = "config"
    PRICES_TABLE = "prices"
    PRICE_ARCHIVE_TABLE = "archive"

    def setup(self, _code: str, _data: dict) -> None:
        engine = create_engine(self.get_engine(_code), echo=False)
        if inspect(engine).has_table(self.CONFIG_TABLE):
            return
        metadata = MetaData()
        Table(self.CONFIG_TABLE,
              metadata,
              Column('key', String()),
              Column('value', String())
              )
        Table(self.PRICES_TABLE,
              metadata,
              Column('price', Float()),
              Column('timestamp', Integer()),
              )
        Table(self.PRICE_ARCHIVE_TABLE,
              metadata,
              Column('price', Float()),
              Column('timestamp', Integer()),
              )
        metadata.create_all(engine)
        with engine.connect() as connection:
            for key, value in _data.items():
                connection.execute(
                    text(f"INSERT INTO {self.CONFIG_TABLE} "
                         "(key, value) VALUES (:key, :value)")
                    .bindparams(key=key, value=value)
                )
            connection.commit()

    def load_prices(self, _coin: Coin) -> Coin:
        engine = create_engine(self.get_engine(_coin.code), echo=False)
        with engine.connect() as connection:
            result = connection.execute(
                text(f"SELECT * FROM {self.PRICES_TABLE}")
            )
        for data in result:
            _coin.price[data[1]] = data[0]
        return _coin

    def load_config(self, _coin: Coin) -> Coin:
        engine = create_engine(self.get_engine(_coin.code), echo=False)
        with engine.connect() as connection:
            result = connection.execute(
                text(f"SELECT * FROM {self.CONFIG_TABLE}")
            )
        for data in result:
            _coin.config[data[0]] = data[1]
        return _coin

    def add_price(self, _coin: Coin, _price: float) -> Coin:
        engine = create_engine(self.get_engine(_coin.code), echo=False)
        timestamp = time()
        with engine.connect() as connection:
            connection.execute(
                text(f"INSERT INTO {self.PRICES_TABLE} "
                     "(price, timestamp) VALUES (:price, :timestamp)")
                .bindparams(price=_price, timestamp=timestamp)
            )
            connection.commit()
        _coin.price[timestamp] = _price
        return _coin

    def move_to_archive(self, _coin: Coin) -> None:
        average_per_hour = []
        key = None
        engine = create_engine(self.get_engine(_coin.code), echo=True)
        with engine.connect() as connection:
            for time, price in _coin.price.items():
                if key is not None and \
                        key != datetime.fromtimestamp(int(time)).strftime('%Y-%m-%d %H'):
                    average_per_hour = sum(average_per_hour) / len(average_per_hour)
                    connection.execute(
                        text(f"INSERT INTO {self.PRICE_ARCHIVE_TABLE} "
                             "(price, timestamp) VALUES (:price, :timestamp)")
                        .bindparams(price=average_per_hour,
                                    timestamp=datetime.fromisoformat(key).timestamp()
                                    )
                    )
                    average_per_hour = []
                key = datetime.fromtimestamp(int(time)).strftime('%Y-%m-%d %H')
                if time > datetime.now().timestamp() - 172800:
                    connection.execute(
                        text(f"DELETE from {self.PRICES_TABLE} "
                             "WHERE timestamp < :timestamp")
                        .bindparams(timestamp=str(datetime.now().timestamp() - 172800))
                    )
                    break
                average_per_hour.append(price)
            connection.commit()
        self.set_config(
            _coin=_coin,
            _key='next_archive',
            _value=str(datetime.now().timestamp() + 172800)
        )
        return

    def set_config(self, _coin: Coin, _key: str, _value: str) -> Coin:
        _coin.config[_key] = _value
        engine = create_engine(self.get_engine(_coin.code), echo=False)
        with engine.connect() as connection:
            connection.execute(
                text(f"DELETE from {self.CONFIG_TABLE} "
                     "WHERE key = :key")
                .bindparams(key=_key)
            )
            connection.execute(
                text(f"INSERT INTO {self.CONFIG_TABLE} "
                     "(key, value) VALUES (:key, :value)")
                .bindparams(key=_key,
                            value=_value
                            )
            )
            connection.commit()
        return _coin

    def get_engine(self, _coin_code: str) -> str:
        env_path = Path(__file__).with_name('data')
        env_path = f"sqlite:///{env_path.absolute()}/{_coin_code}".replace("coins/", "")
        return env_path
