import datetime
from typing import List

from fastapi import APIRouter, Request
from pydantic import BaseModel
import json

from source.util.util_base.constant import FreqCode
from source.util.util_base.date_util import convert_date_to_datetime
from source.util.util_data.manual_data import ManualData

manual_data = APIRouter(prefix="/manual_data")


class SetFutureManualNoteData(BaseModel):
    root_classify_id: str
    classify_id: str
    classify_name: str
    classify_parent_id: str
    note: str
    note_point_data: List  # [['2016-01-01', 10, 20, 5, 30, 100000], [日期, 开盘, 收盘, 最低, 最高, 成交量]..]


@manual_data.post("/set_future_manual_note_data")
async def set_future_manual_note_data(request: Request, data_note_info: SetFutureManualNoteData):
    db_conn = request.state.db_conn

    # data_note_info.note_point_data = "|".join([",".join(row) for row in data_note_info.note_point_data])
    data_note_info.note_point_data = json.dumps(data_note_info.note_point_data)
    await ManualData(db_conn).set_future_manual_note_data(data_note_info.root_classify_id, data_note_info.classify_id, data_note_info.classify_name,
                                                          data_note_info.classify_parent_id, data_note_info.note, data_note_info.note_point_data)


class GetFutureManualNoteData(BaseModel):
    root_classify_id: str


def _struct_tree(parent_node_id, node, data):
    for root_classify_id, classify_id, classify_name, classify_parent_id, note, note_point_data in data:
        if classify_parent_id == parent_node_id:
            node.append({
                "id": classify_id,
                "title": classify_name,
                "note": note,
                "note_point_data": json.loads(note_point_data),
                "children": []
            })
            _struct_tree(classify_id, node[-1]["children"], data)
    return node


@manual_data.post("/get_future_manual_note_data")
async def get_future_manual_note_data(request: Request, data_note_info: GetFutureManualNoteData):
    db_conn = request.state.db_conn

    result = await ManualData(db_conn).get_future_manual_note_data(data_note_info.root_classify_id)
    # result = [
    #     [1, 1, "1", None, "", ""],
    #     [1, 2, "2", 1, "", ""],
    #     [1, 3, "3", 1, "", ""],
    #     [1, 4, "4", 3, "", ""],
    #     [1, 5, "5", 3, "", ""],
    #     [1, 6, "6", 5, "", ""],
    # ]
    result = _struct_tree(None, [], result)
    # root_classify_id, classify_id, classify_name, classify_parent_id, note, note_point_data
    # result = [{"trade_date": i, "note": v, "note_point_data": [row.split(",") for row in h.split("|")]} for i, v, h in result]
    # result = [{"classify_id": i, "classify_name": v, "note_point_data": json.loads(h)} for i, v, h in result]
    return result


@manual_data.post("/get_future_manual_note_data_root_classify_id")
async def get_future_manual_note_data_root_classify_id(request: Request):
    db_conn = request.state.db_conn

    result = await ManualData(db_conn).get_future_manual_note_data_root_classify_id()
    return result


class SetFutureManualTurnNoteData(BaseModel):
    main_ts_code: str
    trade_date: datetime.date
    freq_code: FreqCode
    point_turn_flag: bool
    note: str


@manual_data.post("/set_set_future_manual_turn_note_data")
async def set_set_future_manual_turn_note_data(request: Request, data_note_info: SetFutureManualTurnNoteData):
    db_conn = request.state.db_conn
    data_note_info.trade_date = convert_date_to_datetime(data_note_info.trade_date)

    await ManualData(db_conn).set_set_future_manual_turn_note_data(data_note_info.main_ts_code, data_note_info.trade_date, data_note_info.freq_code,
                                                                   data_note_info.point_turn_flag, data_note_info.note)


class GetFutureManualTurnNoteData(BaseModel):
    main_ts_code: str
    start_date: datetime.date
    end_date: datetime.date
    freq_code: FreqCode


@manual_data.post("/get_set_future_manual_turn_note_data")
async def get_set_future_manual_turn_note_data(request: Request, data_note_info: GetFutureManualTurnNoteData):
    db_conn = request.state.db_conn
    data_note_info.start_date, data_note_info.end_date = convert_date_to_datetime(data_note_info.start_date), convert_date_to_datetime(data_note_info.end_date)

    result = await ManualData(db_conn).get_set_future_manual_turn_note_data(data_note_info.main_ts_code, data_note_info.start_date, data_note_info.end_date,
                                                                            data_note_info.freq_code)
    return result
