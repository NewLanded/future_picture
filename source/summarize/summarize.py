import datetime
from typing import List

from fastapi import APIRouter, Request
from pydantic import BaseModel

from source.util.util_base.constant import FreqCode
from source.util.util_base.date_util import convert_date_to_datetime, obj_contain_datetime_convert_to_str
from source.util.util_data.basic_info import BasicInfo
from source.util.util_data.point_data import PointData
from source.util.util_module.point_module import get_main_code_interval_point_data_by_freq_code

summarize = APIRouter(prefix="/summarize")


class RaiseFallInfo(BaseModel):
    start_date: datetime.date
    end_date: datetime.date
    ts_code_list: List[str]
    freq_code: FreqCode


@summarize.post("/main_code_interval_raise_fall_data")
async def main_code_interval_raise_fall_data(request: Request, summarize_info: RaiseFallInfo):
    """品种上涨下跌数量"""
    summarize_info.start_date, summarize_info.end_date = convert_date_to_datetime(summarize_info.start_date), convert_date_to_datetime(summarize_info.end_date)
    db_conn = request.state.db_conn

    start_date = await BasicInfo(db_conn).get_previous_trade_day(summarize_info.start_date)

    ts_code_point_data = {}
    for ts_code in summarize_info.ts_code_list:
        ts_code_point_data[ts_code] = await get_main_code_interval_point_data_by_freq_code(db_conn, ts_code, start_date, summarize_info.end_date,
                                                                                           summarize_info.freq_code)

    exists_date_list = set()
    for ts_code, ts_code_data in ts_code_point_data.items():
        exists_date_list.update(set(list(ts_code_data)))
    exists_date_list = sorted(list(exists_date_list))

    raise_fall_data = []
    for index in range(1, len(exists_date_list)):
        date_now, date_previous = exists_date_list[index], exists_date_list[index - 1]
        raise_num, fall_num = 0, 0
        for ts_code, ts_code_data in ts_code_point_data.items():
            close_now, close_previous = ts_code_data.get(date_now, {}).get("close", None), ts_code_data.get(date_previous, {}).get("close", None)

            # 不处理没取到数据的
            if close_now is None or close_previous is None:
                continue

            if close_now > close_previous:
                raise_num += 1
            else:
                fall_num += 1

        raise_fall_data.append({
            "date": date_now,
            "raise_num": raise_num,
            "fall_num": fall_num
        })

    return obj_contain_datetime_convert_to_str(raise_fall_data)


class PointPercentInfo(BaseModel):
    data_date: datetime.date
    ts_code: str


@summarize.post("/main_code_point_percent")
async def main_code_interval_raise_fall_data(request: Request, summarize_info: PointPercentInfo):
    """品种点位在历史点位(10年)位置"""
    summarize_info.data_date = convert_date_to_datetime(summarize_info.data_date)
    db_conn = request.state.db_conn

    point_result = await PointData(db_conn).get_future_max_min_now_point_by_main_code(summarize_info.ts_code, summarize_info.data_date - datetime.timedelta(days=3650),
                                                                                      summarize_info.data_date)
    point_percent = (point_result["point_now"] - point_result["min_point"]) / (point_result["max_point"] - point_result["min_point"])
    return point_percent
