from enum import Enum

EXCHANGE = {
    "CZCE": "郑州商品交易所",
    "DCE": "大连商品交易所",
    "SHFE": "上海期货交易所",
    "CFFEX": "中国金融期货交易所"
}


class FreqCode(Enum):
    DAY = "D"
    WEEK = "W"
    MONTH = "M"
