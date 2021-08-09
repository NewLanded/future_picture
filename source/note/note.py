import datetime
import os
from typing import List, Dict
import json

from fastapi import APIRouter, Query, Request
from pydantic import BaseModel, Field
from source.config import NOTE_DIR
from source.util.util_base.constant import FreqCode
from source.util.util_data.note_data import NoteData

note = APIRouter(prefix="/note", tags=["记录"])


class NoteWriteRequest(BaseModel):
    main_ts_code: str
    ts_code: str
    freq_code: FreqCode
    trade_date: datetime.date
    note: str


@note.post("/note_write", response_model=None)
async def note_write(request: Request, request_params: NoteWriteRequest):
    """
    记录写入
    """
    db_conn = request.state.db_conn
    # trade_date = convert_date_to_datetime(request_params.trade_date)

    await NoteData(db_conn).note_insert(request_params.main_ts_code, request_params.ts_code, request_params.freq_code, request_params.trade_date, request_params.note)


class BsNoteWriteRequest(BaseModel):
    main_ts_code: str
    ts_code: str
    freq_code: FreqCode
    trade_date: datetime.date
    trade_type: str = Field(..., example="long_open long_gallon long_reduce long_close   short_open short_gallon short_reduce short_close")
    number: int
    point: float
    note: str


@note.post("/bs_note_write", response_model=None)
async def bs_note_write(request: Request, request_params: BsNoteWriteRequest):
    """
    交易记录写入
    """
    db_conn = request.state.db_conn
    # trade_date = convert_date_to_datetime(request_params.trade_date)

    await NoteData(db_conn).bs_note_insert(request_params.main_ts_code, request_params.ts_code, request_params.freq_code, request_params.trade_date, request_params.trade_type,
                                           request_params.number,
                                           request_params.point, request_params.note)


class GetNoteRequest(BaseModel):
    main_ts_code: str
    start_date: datetime.date
    end_date: datetime.date


class GetNoteResponse(BaseModel):
    main_ts_code: str
    ts_code: str
    freq_code: FreqCode
    trade_date: datetime.date
    note: str


@note.post("/get_note", response_model=List[GetNoteResponse])
async def get_note(request: Request, request_params: GetNoteRequest):
    """
    记录查询
    """
    db_conn = request.state.db_conn
    # start_date, end_date = convert_date_to_datetime(request_params.start_date), convert_date_to_datetime(request_params.end_date)

    result = await NoteData(db_conn).get_note(request_params.main_ts_code, request_params.start_date, request_params.end_date)
    return result


class GetBsNoteRequest(BaseModel):
    main_ts_code: str
    start_date: datetime.date
    end_date: datetime.date


class GetBsNoteResponse(BaseModel):
    main_ts_code: str
    ts_code: str
    freq_code: FreqCode
    trade_date: datetime.date
    trade_type: str
    number: int
    point: float
    note: str


@note.post("/get_bs_note", response_model=List[GetBsNoteResponse])
async def get_bs_note(request: Request, request_params: GetBsNoteRequest):
    """
    交易记录查询
    """
    db_conn = request.state.db_conn
    # start_date, end_date = convert_date_to_datetime(request_params.start_date), convert_date_to_datetime(request_params.end_date)

    result = await NoteData(db_conn).get_bs_note(request_params.main_ts_code, request_params.start_date, request_params.end_date)
    return result


class GetFileNoteRequest(BaseModel):
    file_name: str


@note.post("/get_file_note", response_model=str)
async def get_file_note(request: Request, request_params: GetFileNoteRequest):
    """
    读取文件
    """
    # aiofiles 异步读取
    file_path = os.path.join(NOTE_DIR, request_params.file_name)
    if os.path.commonprefix((os.path.realpath(file_path), os.path.realpath(NOTE_DIR))) != NOTE_DIR:
        raise ValueError("文件路径不符合要求")

    with open(file_path, encoding='utf-8') as f:
        result = f.read()

    return result


@note.post("/get_file_note_split", response_model=List[str])
async def get_file_note_split(request: Request, request_params: GetFileNoteRequest):
    result = await get_file_note(request, request_params)
    result = list(filter(lambda x: x.strip(), result.split("\n")))
    return result


@note.post("/get_file_note_json")
async def get_file_note_json(request: Request, request_params: GetFileNoteRequest):
    result = await get_file_note(request, request_params)
    result = json.loads(result)
    return result


class GetJsonDataRequest(BaseModel):
    key_name: str


@note.post("/get_json_data")
async def get_json_data(request: Request, request_params: GetJsonDataRequest):
    """
    表中json数据查询
    """
    db_conn = request.state.db_conn

    result = await NoteData(db_conn).get_json_data(request_params.key_name)
    return result


class GetStrategyResultDataRequest(BaseModel):
    trade_date: datetime.date


class GetStrategyResultDataResponse(BaseModel):
    strategy_code: str
    data: List[Dict[str, str]]


@note.post("/get_strategy_result_data", response_model=List[GetStrategyResultDataResponse])
async def get_strategy_result_data(request: Request, request_params: GetStrategyResultDataRequest):
    """
    策略结果表 数据查询
    """
    db_conn = request.state.db_conn

    result_ori = await NoteData(db_conn).get_strategy_result_data(request_params.trade_date)
    result = []
    for strategy_code, strategy_data in result_ori.items():
        result.append({
            "strategy_code": strategy_code,
            "data": strategy_data
        })
        result[-1]["data"].sort(key=lambda x: x["freq_code"])
    result.sort(key=lambda x: x["strategy_code"])

    return result
