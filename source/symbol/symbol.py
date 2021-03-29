import datetime

from fastapi import APIRouter, Request
from pydantic import BaseModel

from source.util.util_base.date_util import convert_date_to_datetime
from source.util.util_data.basic_info import BasicInfo

symbol = APIRouter(prefix="/symbol")


class SymbolDateInfo(BaseModel):
    data_date: datetime.date


@symbol.post("/active_symbol_info")
async def active_symbol_info(request: Request, symbol_info: SymbolDateInfo):
    db_conn = request.state.db_conn

    data_date = convert_date_to_datetime(symbol_info.data_date)

    active_ts_code_info = await BasicInfo(db_conn).get_active_ts_code_info(data_date)

    return active_ts_code_info


class SymbolCodeInfo(BaseModel):
    ts_code: str


@symbol.post("/get_main_ts_code_by_ts_code")
async def get_main_ts_code_by_ts_code(request: Request, symbol_info: SymbolCodeInfo):
    db_conn = request.state.db_conn

    main_ts_code = await BasicInfo(db_conn).get_main_ts_code_by_ts_code(symbol_info.ts_code)

    return main_ts_code


class SymbolCodeInfo2(BaseModel):
    main_ts_code: str
    data_date: datetime.date


@symbol.post("/get_ts_code_by_main_ts_code")
async def get_ts_code_by_main_ts_code(request: Request, symbol_info: SymbolCodeInfo2):
    symbol_info.data_date = convert_date_to_datetime(symbol_info.data_date)
    db_conn = request.state.db_conn

    ts_code = await BasicInfo(db_conn).get_ts_code_by_main_ts_code(symbol_info.main_ts_code, symbol_info.data_date)

    return ts_code
