import re
from itertools import count

from fastapi import HTTPException, status

from source.util.util_base.db import get_multi_data, get_single_value


class BasicInfo:
    def __init__(self, db_conn):
        self.db_conn = db_conn

    async def get_ts_code_by_main_ts_code(self, main_ts_code, data_date):
        sql = """
        select mapping_ts_code from future_main_code_data
        where ts_code = %s and trade_date = %s;
        """
        args = [main_ts_code, data_date]
        result = await get_single_value(self.db_conn, sql, args)
        return result

    async def get_main_ts_code_by_ts_code(self, ts_code):
        sql = """
        select ts_code from future_main_code_data
        where mapping_ts_code = %s;
        """
        args = [ts_code]
        result = await get_single_value(self.db_conn, sql, args)
        return result

    async def get_active_ts_code_info(self, data_date):
        ts_code_list = await self.get_active_ts_code(data_date)
        if not ts_code_list:
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="未获取到有效的ts_code")

        sql = """
        select ts_code, exchange, name from future_basic_info_data
        where ts_code in %s;
        """
        args = [ts_code_list]
        result = await get_multi_data(self.db_conn, sql, args)
        return result

    async def get_active_ts_code(self, data_date):
        data_date = await self.get_active_trade_day(data_date)
        sql = """
        select ts_code, mapping_ts_code from future_main_code_data
        where trade_date = %s;
        """
        args = [data_date]
        result = await get_multi_data(self.db_conn, sql, args)

        result_new = []
        for row in result:
            symbol_ts, symbol_main = row[0].split('.')[0], row[1].split('.')[0]

            if re.match(r'[A-Z]*', symbol_ts).group(0) == re.match(r'[A-Z]*', symbol_main).group(0):
                result_new.append(row[1])

        result = list(set(result_new))
        result.sort()

        return result

    async def get_future_info_by_symbol(self, symbol_code_list):
        index_dict = dict(zip([i for i in symbol_code_list], count()))

        sql = """
        select ts_code, name from  future_basic_info_data 
        where symbol in %s;
        """
        args = [symbol_code_list]
        result = await get_multi_data(self.db_conn, sql, args)

        result = sorted(result, key=lambda x: index_dict[x[0].split('.')[0]])

        return result

    async def get_active_trade_day(self, data_date):
        sql = """
        select max(date) from sec_date_info where date <= %s and is_workday_flag=1;
        """
        args = [data_date]
        date = await get_single_value(self.db_conn, sql, args)
        return date

    async def get_previous_trade_day(self, data_date):
        sql = """
        select max(date) from sec_date_info where date < %s and is_workday_flag=1;
        """
        args = [data_date]
        date = await get_single_value(self.db_conn, sql, args)
        return date

    async def get_next_trade_day(self, data_date):
        sql = """
        select min(date) from sec_date_info where date > %s and is_workday_flag=1;
        """
        args = [data_date]
        date = await get_single_value(self.db_conn, sql, args)
        return date