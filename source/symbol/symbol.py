import datetime
from typing import List
from fastapi import APIRouter, Request
from pydantic import BaseModel

from source.util.util_base.date_util import convert_date_to_datetime
from source.util.util_data.basic_info import BasicInfo

symbol = APIRouter(prefix="/symbol", tags=["产品代码"])


class ActiveSymbolInfoRequest(BaseModel):
    data_date: datetime.date


class ActiveSymbolInfoResponse(BaseModel):
    ts_code: str
    exchange: str
    name: str


@symbol.post("/active_symbol_info", response_model=List[ActiveSymbolInfoResponse])
async def active_symbol_info(request: Request, symbol_info: ActiveSymbolInfoRequest):
    """
    获取当前生效的ts_code
    """
    db_conn = request.state.db_conn

    data_date = convert_date_to_datetime(symbol_info.data_date)

    active_ts_code_info_raw = await BasicInfo(db_conn).get_active_ts_code_info(data_date)
    active_ts_code_info = []
    for ts_code, exchange, name in active_ts_code_info_raw:
        active_ts_code_info.append({
            "ts_code": ts_code,
            "exchange": exchange,
            "name": name
        })

    return active_ts_code_info


class SymbolCodeInfo(BaseModel):
    ts_code: str


@symbol.post("/get_main_ts_code_by_ts_code", response_model=str)
async def get_main_ts_code_by_ts_code(request: Request, symbol_info: SymbolCodeInfo):
    """
    使用ts_code获取其连续 ts_code 代码
    """
    db_conn = request.state.db_conn

    main_ts_code = await BasicInfo(db_conn).get_main_ts_code_by_ts_code(symbol_info.ts_code)

    return main_ts_code


class SymbolCodeInfo2(BaseModel):
    main_ts_code: str
    data_date: datetime.date


@symbol.post("/get_ts_code_by_main_ts_code", response_model=str)
async def get_ts_code_by_main_ts_code(request: Request, symbol_info: SymbolCodeInfo2):
    """
    使用连续ts_code代码获取其在日期对应的ts_code
    """
    symbol_info.data_date = convert_date_to_datetime(symbol_info.data_date)
    db_conn = request.state.db_conn

    ts_code = await BasicInfo(db_conn).get_ts_code_by_main_ts_code(symbol_info.main_ts_code, symbol_info.data_date)

    return ts_code
