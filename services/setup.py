from coins.repository import Repository
from binance import Client
import env


class Setup:

    def execute(self):
        coins_repo = Repository()

        client = Client(
            env.env['api_key'],
            env.env['api_secret']
        )
        prices = client.get_exchange_info()
        for asset in prices['symbols']:
            asset_data = {
                'status': '1',
                'min_price': '',
                'min_qty': ''
            }
            if asset['status'] != 'TRADING' \
                    or asset['quoteAsset'] != env.env['base_coin']:
                continue
            for _filter in asset['filters']:
                if _filter['filterType'] == "PRICE_FILTER":
                    asset_data['min_price'] = _filter['minPrice']
                if _filter['filterType'] == "LOT_SIZE":
                    asset_data['min_qty'] = _filter['minQty']
            coins_repo.setup(_coin_code=asset['baseAsset'], _data=asset_data)

