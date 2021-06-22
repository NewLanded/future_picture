import datetime
import os
from typing import List

from fastapi import APIRouter, Request, Query
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
    file_name: str = Query("normal_note")


@note.post("/get_file_note", response_model=List[str])
async def get_bs_note(request: Request, request_params: GetFileNoteRequest):
    """
    交易记录查询
    """
    # aiofiles 异步读取
    with open(os.path.join(NOTE_DIR, request_params.file_name)) as f:
        result = f.read()

    result = list(filter(lambda x: x.strip(), result.split("\n")))
    return result
