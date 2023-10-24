from sys import argv
from services.load_prices import LoadPrices
from services.setup import Setup
from services.analyze import Analyze


def process_command(command_: str):
    if command_ == "load-prices":
        load_prices = LoadPrices()
        load_prices.execute()
    if command_ == "setup":
        Setup().execute()
    if command_ == "analyze":
        print(Analyze().execute(14))


for command in argv:
    process_command(command)

