from sys import argv
from services.load_prices import LoadPrices
from services.setup import Setup


def process_command(command_: str):
    if command_ == "load-prices":
        counter = 0
        load_prices = LoadPrices()
        while counter < 11:
            load_prices.execute()
            counter += 1
    if command_ == "setup":
        Setup().execute()


for command in argv:
    process_command(command)

