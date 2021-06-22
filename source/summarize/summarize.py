import datetime
from typing import List, Dict

from fastapi import APIRouter, Request
from pydantic import BaseModel, Field

from source.util.util_base.constant import FreqCode
from source.util.util_base.date_util import convert_date_to_datetime, obj_contain_datetime_convert_to_str
from source.util.util_data.basic_info import BasicInfo
from source.util.util_data.point_data import PointData
from source.util.util_module.point_module import get_main_code_interval_point_data_by_freq_code, get_ts_code_interval_point_data_by_freq_code

summarize = APIRouter(prefix="/summarize", tags=["汇总数据"])


class RaiseFallInfoRequest(BaseModel):
    start_date: datetime.date
    end_date: datetime.date
    ts_code_list: List[str] = Field(..., example=["A.DCE", "B2105.DCE"])
    freq_code: FreqCode


class RaiseFallInfoResponse(BaseModel):
    date: datetime.date
    raise_num: int
    fall_num: int


@summarize.post("/main_code_interval_raise_fall_data", response_model=List[RaiseFallInfoResponse])
async def main_code_interval_raise_fall_data(request: Request, summarize_info: RaiseFallInfoRequest):
    """
    品种上涨下跌数量, 未取到数据的品种不处理
    """
    # raise HTTPException(status_code=500, detail="Item not found")

    # summarize_info.start_date, summarize_info.end_date = convert_date_to_datetime(summarize_info.start_date), convert_date_to_datetime(summarize_info.end_date)
    db_conn = request.state.db_conn

    start_date = await BasicInfo(db_conn).get_previous_trade_day(summarize_info.start_date)
    start_date = start_date if start_date else summarize_info.start_date

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
    ts_code: str = Field(..., example="A.DCE")


@summarize.post("/main_code_point_percent", response_model=float)
async def main_code_point_percent(request: Request, summarize_info: PointPercentInfo):
    """品种点位在历史点位(10年)位置"""
    # summarize_info.data_date = convert_date_to_datetime(summarize_info.data_date)
    db_conn = request.state.db_conn

    point_result = await PointData(db_conn).get_future_max_min_now_point_by_main_code(summarize_info.ts_code, summarize_info.data_date - datetime.timedelta(days=3650),
                                                                                      summarize_info.data_date)
    point_percent = (point_result["point_now"] - point_result["min_point"]) / (point_result["max_point"] - point_result["min_point"])
    return point_percent


class MainCodeClosePointRequest(BaseModel):
    start_date: datetime.date
    end_date: datetime.date
    ts_code_list: List[str] = Field(..., example=["A.DCE", "B2105.DCE"])
    freq_code: FreqCode


class MainCodeClosePointResponse(BaseModel):
    ts_code: str
    data: List[Dict]


@summarize.post("/main_code_relative_close_point_data", response_model=List[MainCodeClosePointResponse])
async def main_code_relative_close_point_data(request: Request, summarize_info: MainCodeClosePointRequest):
    """
    以1000为基准, 获取收盘价的相对值
    :return:
    ```
    [
        {
            ts_code: A.DCE,
            data: [
                {
                    date: 2021-04-05,
                    point: 123
                },
                ...
            ]
        },
        ...
    ]
    ```
    """
    # summarize_info.start_date, summarize_info.end_date = convert_date_to_datetime(summarize_info.start_date), convert_date_to_datetime(summarize_info.end_date)
    db_conn = request.state.db_conn

    ts_code_point_data = {}
    for ts_code in summarize_info.ts_code_list:
        ts_code_point_data[ts_code] = await get_main_code_interval_point_data_by_freq_code(db_conn, ts_code, summarize_info.start_date, summarize_info.end_date,
                                                                                           summarize_info.freq_code)
        min_date = min(ts_code_point_data[ts_code])
        min_date_percent = ts_code_point_data[ts_code][min_date]['close'] / 1000
        for date, date_data in ts_code_point_data[ts_code].items():
            date_data['close'] = date_data['close'] / min_date_percent - 1000

    result = []
    for ts_code, ts_code_data in ts_code_point_data.items():
        result.append({"ts_code": ts_code, "data": []})
        for date, date_data in ts_code_data.items():
            result[-1]["data"].append({
                "date": date,
                "point": date_data["close"]
            })
        result[-1]["data"].sort(key=lambda x: x["date"])

    return obj_contain_datetime_convert_to_str(result)


class TsCodeClosePointDataRelativeByOldestCodeRequest(BaseModel):
    start_date: datetime.date
    end_date: datetime.date
    ts_code_list: List[str] = Field(..., example=["A2105.DCE", "B2105.DCE"])
    freq_code: FreqCode


class TsCodeClosePointDataRelativeByOldestCodeResponse(BaseModel):
    date: List[datetime.date]
    data: List[Dict]


@summarize.post("/ts_code_close_point_data_relative_by_oldest_code", response_model=TsCodeClosePointDataRelativeByOldestCodeResponse)
async def ts_code_close_point_data_relative_by_oldest_code(request: Request, summarize_info: TsCodeClosePointDataRelativeByOldestCodeRequest):
    """
    以最远的, 有数的的合约为基准, 获取收盘价的相对值, 基准合约没有的日期, 则放弃
    :return:
    ```
    {
        date: [2016-01-01, ...]
        data: [
            {
                ts_code: A2105.DCE,
                point: [2107, ...]
            }
        ]
    }
    ```
    """
    db_conn = request.state.db_conn

    ts_code_point_data = {}
    for ts_code in summarize_info.ts_code_list:
        ts_code_point_data[ts_code] = await get_ts_code_interval_point_data_by_freq_code(db_conn, ts_code, summarize_info.start_date, summarize_info.end_date,
                                                                                         summarize_info.freq_code)
    benm_ts_code = min(list(ts_code_point_data))
    benm_ts_code_date_list = sorted(list(ts_code_point_data[benm_ts_code]))

    benm_ts_code_date_set = set(benm_ts_code_date_list)
    for ts_code, ts_code_data in ts_code_point_data.items():
        for date_now in list(ts_code_data):
            if date_now not in benm_ts_code_date_set:
                ts_code_data.pop(date_now)

    ts_code_point_data_format = {"date": benm_ts_code_date_list, "data": []}
    for ts_code, ts_code_data in ts_code_point_data.items():
        previous_point = 0
        ts_code_point_data_now = {"ts_code": ts_code, "point": []}
        for date_now in benm_ts_code_date_list:
            if date_now in ts_code_point_data[ts_code]:
                ts_code_point_data_now["point"].append(ts_code_point_data[ts_code][date_now]["close"] - ts_code_point_data[benm_ts_code][date_now]["close"])
            else:
                ts_code_point_data_now["point"].append(previous_point)

            previous_point = ts_code_point_data_now["point"][-1]

        ts_code_point_data_format["data"].append(ts_code_point_data_now)

    return ts_code_point_data_format
