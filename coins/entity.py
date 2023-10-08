class Entity:
    def __init__(self, _code: str):
        self.code = _code  # code of coin (BTC, LTC, etc.)
        self.config = {}  # general data about coin like lot size, min.price
        self.price = {}  # price of coin for last 48 hours
        self.archive = {}  # price archive for prices older than 48 hours
        return
