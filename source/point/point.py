import datetime
from typing import List, Dict

from fastapi import APIRouter, Request
from pydantic import BaseModel, Field

from source.util.util_base.constant import FreqCode
from source.util.util_base.date_util import convert_date_to_datetime, obj_contain_datetime_convert_to_str
from source.util.util_module.point_module import get_main_code_interval_point_data_by_freq_code, get_ts_code_interval_point_data_by_freq_code, \
    get_ts_code_interval_pure_holding_data

point = APIRouter(prefix="/point", tags=["点位数据"])


class MainCodeIntervalPointDataRequest(BaseModel):
    start_date: datetime.date
    end_date: datetime.date
    ts_code: str = Field(..., example="A.DCE")
    freq_code: FreqCode


class MainCodeIntervalPointDataResponse(BaseModel):
    date: datetime.date
    ts_code: str
    open: float
    high: float
    low: float
    close: float
    vol: int


@point.post("/main_code_interval_point_data", response_model=List[MainCodeIntervalPointDataResponse])
async def main_code_interval_point_data(request: Request, point_info: MainCodeIntervalPointDataRequest):
    # point_info.start_date, point_info.end_date = convert_date_to_datetime(point_info.start_date), convert_date_to_datetime(point_info.end_date)
    db_conn = request.state.db_conn

    point_data_raw = await get_main_code_interval_point_data_by_freq_code(db_conn, point_info.ts_code, point_info.start_date, point_info.end_date, point_info.freq_code)
    point_data = []
    for date, date_data in point_data_raw.items():
        point_data.append({
            "date": date,
            "ts_code": date_data["ts_code"],
            "open": date_data["open"],
            "high": date_data["high"],
            "low": date_data["low"],
            "close": date_data["close"],
            "vol": date_data["vol"]
        })

    return obj_contain_datetime_convert_to_str(point_data)


class TsCodeIntervalPointDataRequest(BaseModel):
    start_date: datetime.date
    end_date: datetime.date
    ts_code: str = Field(..., example="A2105.DCE")
    freq_code: FreqCode


class TsCodeIntervalPointDataResponse(BaseModel):
    date: datetime.date
    ts_code: str
    open: float
    high: float
    low: float
    close: float
    vol: int


@point.post("/ts_code_interval_point_data", response_model=List[TsCodeIntervalPointDataResponse])
async def ts_code_interval_point_data(request: Request, point_info: TsCodeIntervalPointDataRequest):
    """
    表中存在main_code的数据, 所以ts_code可以传main_code
    """
    # point_info.start_date, point_info.end_date = convert_date_to_datetime(point_info.start_date), convert_date_to_datetime(point_info.end_date)
    db_conn = request.state.db_conn

    point_data_raw = await get_ts_code_interval_point_data_by_freq_code(db_conn, point_info.ts_code, point_info.start_date, point_info.end_date, point_info.freq_code)
    point_data = []
    for date, date_data in point_data_raw.items():
        point_data.append({
            "date": date,
            "ts_code": date_data["ts_code"],
            "open": date_data["open"],
            "high": date_data["high"],
            "low": date_data["low"],
            "close": date_data["close"],
            "vol": date_data["vol"]
        })

    return obj_contain_datetime_convert_to_str(point_data)


class HoldingInfo(BaseModel):
    start_date: datetime.date
    end_date: datetime.date
    ts_code: str


class TsCodeIntervalPureHoldingDataResponse(BaseModel):
    date: datetime.date
    long: List[Dict] = Field(..., example=[{"broker": "海通期货", "amount": 18045, "chg": -528, "percent": 0.07219212827755063}])
    short: List[Dict] = Field(..., example=[{"broker": "海通期货", "amount": 18045, "chg": -528, "percent": 0.07219212827755063}])
    vol: List[Dict] = Field(..., example=[{"broker": "海通期货", "amount": 18045, "chg": -528, "percent": 0.07219212827755063}])


@point.post("/ts_code_interval_pure_holding_data", response_model=List[TsCodeIntervalPureHoldingDataResponse])
async def ts_code_interval_pure_holding_data(request: Request, holding_info: HoldingInfo):
    """
    表中存在main_code的数据, 所以ts_code可以传main_code
    :param request:
    :param holding_info:
    :return:
    ```
        [
          {
            "date": "2021-04-01",
            "long": [
              {
                "broker": "海通期货",
                "amount": 18045,
                "chg": -528,
                "percent": 0.07219212827755063
              },
              ...
            ],
            "short": [
              {
                "broker": "鲁证期货",
                "amount": 18861,
                "chg": 1064,
                "percent": 0.06866535605067715
              },
              ...
            ],
            "vol": [
              {
                "broker": "海通期货",
                "amount": 134333,
                "chg": -95193,
                "percent": 0.21055263149645298
              },
              ...
            ]
          }
        ]
    ```
    """
    # holding_info.start_date, holding_info.end_date = convert_date_to_datetime(holding_info.start_date), convert_date_to_datetime(holding_info.end_date)
    db_conn = request.state.db_conn
    holding_data_ori = await get_ts_code_interval_pure_holding_data(db_conn, holding_info.ts_code, holding_info.start_date, holding_info.end_date)

    sum_holding_data = {}
    for date, date_value in holding_data_ori.items():
        for key, key_value in date_value.items():
            sum_holding_data.setdefault(date, {}).setdefault(key, 0)
            for row in key_value:
                sum_holding_data[date][key] += row['amount']

    holding_data = []
    for date, date_value in holding_data_ori.items():
        holding_data.append({"date": date})
        for key, key_value in date_value.items():
            for row in key_value:
                row['percent'] = row['amount'] / sum_holding_data[date][key]

            key_value.sort(key=lambda x: x['amount'], reverse=True)
            holding_data[-1][key] = key_value

    holding_data.sort(key=lambda x: x['date'])
    return obj_contain_datetime_convert_to_str(holding_data)


def _sum_hold_data(first_n, data, key):
    data = data[:first_n]
    result = sum(i[key] for i in data)
    return result


class TsCodeIntervalPureHoldingDataFirstNResponse(BaseModel):
    first_n: int
    data: List[Dict] = Field(..., example=[{"date": "2021-04-02", "long": 18236, "long_percent": 0.07072987208427389, "short": 19543, "short_percent": 0.06977126108082442}])


@point.post("/ts_code_interval_pure_holding_data_first_n", response_model=List[TsCodeIntervalPureHoldingDataFirstNResponse])
async def ts_code_interval_pure_holding_data_first_n(request: Request, holding_info: HoldingInfo):
    """
    表中存在main_code的数据, 所以ts_code可以传main_code
    获取品种前N持仓及其占比
    :param request:
    :param holding_info:
    :return:
    ```
        [
          {
            "first_n": 1,
            "data": [
              {
                "date": "2021-04-02",
                "long": 18236,
                "long_percent": 0.07072987208427389,
                "short": 19543,
                "short_percent": 0.06977126108082442
              }
            ]
          },
          {
            "first_n": 3,
            "data": [
              {
                "date": "2021-04-02",
                "long": 52389,
                "long_percent": 0.20319517814339905,
                "short": 56001,
                "short_percent": 0.19993145329720352
              }
            ]
          },
          {
            "first_n": 20,
            "data": [
              {
                "date": "2021-04-02",
                "long": 257826,
                "long_percent": 1,
                "short": 280101,
                "short_percent": 0.9999999999999999
              }
            ]
          }
        ]
    ```
    """
    holding_data_ori = await ts_code_interval_pure_holding_data(request, holding_info)

    holding_data = [{"first_n": 1, "data": []}, {"first_n": 3, "data": []}, {"first_n": 20, "data": []}]
    for date_value in holding_data_ori:
        holding_data[0]["data"].append(
            {"date": date_value["date"], "long": _sum_hold_data(1, date_value['long'], 'amount'), "long_percent": _sum_hold_data(1, date_value['long'], 'percent'),
             "short": _sum_hold_data(1, date_value['short'], 'amount'), "short_percent": _sum_hold_data(1, date_value['short'], 'percent')})
        holding_data[1]["data"].append(
            {"date": date_value["date"], "long": _sum_hold_data(3, date_value['long'], 'amount'), "long_percent": _sum_hold_data(3, date_value['long'], 'percent'),
             "short": _sum_hold_data(3, date_value['short'], 'amount'), "short_percent": _sum_hold_data(3, date_value['short'], 'percent')})
        holding_data[2]["data"].append(
            {"date": date_value["date"], "long": _sum_hold_data(20, date_value['long'], 'amount'), "long_percent": _sum_hold_data(20, date_value['long'], 'percent'),
             "short": _sum_hold_data(20, date_value['short'], 'amount'), "short_percent": _sum_hold_data(20, date_value['short'], 'percent')})

    return obj_contain_datetime_convert_to_str(holding_data)


class TsCodeIntervalPureVolumeDataResponse(BaseModel):
    long: List[Dict] = Field(..., example=[{"amount": 12711, "percent": 0.04930069116380815, "broker": "一德期货", "date": "2021-04-02"}])
    short: List[Dict] = Field(..., example=[{"amount": 12711, "percent": 0.04930069116380815, "broker": "一德期货", "date": "2021-04-02"}])
    vol: List[Dict] = Field(..., example=[{"amount": 12711, "percent": 0.04930069116380815, "broker": "一德期货", "date": "2021-04-02"}])


@point.post("/ts_code_interval_pure_volume_data", response_model=TsCodeIntervalPureVolumeDataResponse)
async def ts_code_interval_pure_volume_data(request: Request, holding_info: HoldingInfo):
    """
    表中存在main_code的数据, 所以ts_code可以传main_code
    获取品种持仓及其占比, 获取的是在龙虎榜中的占比
    :param request:
    :param holding_info:
    :return:
    ```
        {
          "long": [
            {
              "amount": 12711,
              "percent": 0.04930069116380815,
              "broker": "一德期货",
              "date": "2021-04-02"
            },
            ...
          ],
          "short": [
            {
              "amount": 11528,
              "percent": 0.04115658280405996,
              "broker": "一德期货",
              "date": "2021-04-02"
            },
            ...
          ],
          "vol": [
            {
              "amount": 44112,
              "percent": 0.06236806944866638,
              "broker": "东吴期货",
              "date": "2021-04-02"
            },
            ...
          ]
        }
    ```
    """
    # holding_info.start_date, holding_info.end_date = convert_date_to_datetime(holding_info.start_date), convert_date_to_datetime(holding_info.end_date)
    db_conn = request.state.db_conn

    holding_data_ori = await get_ts_code_interval_pure_holding_data(db_conn, holding_info.ts_code, holding_info.start_date, holding_info.end_date)

    sum_holding_data_ori = {}
    for date, date_value in holding_data_ori.items():
        for key, key_value in date_value.items():
            sum_holding_data_ori.setdefault(date, {}).setdefault(key, 0)
            for row in key_value:
                sum_holding_data_ori[date][key] += row['amount']

    volume_data = {}
    for date, date_value in holding_data_ori.items():
        for key, key_value in date_value.items():
            for row in key_value:
                volume_data.setdefault(date, {}).setdefault(key, []).append(
                    {'amount': row['amount'], 'percent': row['amount'] / sum_holding_data_ori[date][key], 'broker': row['broker']})

    volume_data_formated = {}
    for date, date_value in volume_data.items():
        for key, key_value in date_value.items():
            volume_data_formated.setdefault(key, [])
            for row in key_value:
                volume_data_formated[key].append(row)
                volume_data_formated[key][-1]['date'] = date

    return obj_contain_datetime_convert_to_str(volume_data_formated)
