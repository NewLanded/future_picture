import datetime

from fastapi import APIRouter, Request
from pydantic import BaseModel

from source.util.util_base.constant import FreqCode
from source.util.util_base.date_util import convert_date_to_datetime, obj_contain_datetime_convert_to_str
from source.util.util_data.point_data import PointData
from source.util.util_module.point_module import get_main_code_interval_point_data_by_freq_code, get_ts_code_interval_point_data_by_freq_code, get_ts_code_interval_holding_data

point = APIRouter(prefix="/point")


class PointInfo(BaseModel):
    start_date: datetime.date
    end_date: datetime.date
    ts_code: str
    freq_code: FreqCode


@point.post("/main_code_interval_point_data")
async def main_code_interval_point_data(request: Request, point_info: PointInfo):
    point_info.start_date, point_info.end_date = convert_date_to_datetime(point_info.start_date), convert_date_to_datetime(point_info.end_date)
    db_conn = request.state.db_conn

    point_data = await get_main_code_interval_point_data_by_freq_code(db_conn, point_info.ts_code, point_info.start_date, point_info.end_date, point_info.freq_code)

    return obj_contain_datetime_convert_to_str(point_data)


@point.post("/ts_code_interval_point_data")
async def ts_code_interval_point_data(request: Request, point_info: PointInfo):
    point_info.start_date, point_info.end_date = convert_date_to_datetime(point_info.start_date), convert_date_to_datetime(point_info.end_date)
    db_conn = request.state.db_conn

    point_data = await get_ts_code_interval_point_data_by_freq_code(db_conn, point_info.ts_code, point_info.start_date, point_info.end_date, point_info.freq_code)

    return obj_contain_datetime_convert_to_str(point_data)


class HoldingInfo(BaseModel):
    start_date: datetime.date
    end_date: datetime.date
    ts_code: str


@point.post("/ts_code_interval_holding_data")
async def ts_code_interval_holding_data(request: Request, holding_info: HoldingInfo):
    holding_info.start_date, holding_info.end_date = convert_date_to_datetime(holding_info.start_date), convert_date_to_datetime(holding_info.end_date)
    db_conn = request.state.db_conn
    holding_data_ori = await get_ts_code_interval_holding_data(db_conn, holding_info.ts_code, holding_info.start_date, holding_info.end_date)

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


@point.post("/ts_code_interval_holding_data_first_n")
async def ts_code_interval_holding_data_first_n(request: Request, holding_info: HoldingInfo):
    holding_data_ori = await ts_code_interval_holding_data(request, holding_info)

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


@point.post("/ts_code_interval_volume_data")
async def ts_code_interval_volume_data(request: Request, holding_info: HoldingInfo):
    holding_info.start_date, holding_info.end_date = convert_date_to_datetime(holding_info.start_date), convert_date_to_datetime(holding_info.end_date)
    db_conn = request.state.db_conn

    holding_data_ori = await get_ts_code_interval_holding_data(db_conn, holding_info.ts_code, holding_info.start_date, holding_info.end_date)

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
