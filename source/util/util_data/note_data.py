import datetime
import json

from source.util.util_base.db import (get_multi_data, get_single_value,
                                      update_data)
from source.util.util_data.basic_info import BasicInfo


class NoteData:
    def __init__(self, db_conn):
        self.db_conn = db_conn

    async def note_insert(self, main_ts_code, ts_code, freq_code, trade_date, note):
        sql = """
        insert into future_note_data(main_ts_code, ts_code, freq_code, trade_date, note, update_date) values ($1, $2, $3, $4, $5, $6)
        """
        args = [main_ts_code, ts_code, freq_code.value, trade_date, note, datetime.datetime.now()]

        await update_data(self.db_conn, sql, args)

    async def get_note(self, main_ts_code, start_date, end_date):
        sql = """
        select main_ts_code, ts_code, freq_code, trade_date, note from
        (select main_ts_code, ts_code, freq_code, trade_date, note from future_note_data 
        where main_ts_code = $1 and trade_date between $2 and $3 
        union
        select main_ts_code, ts_code, freq_code, trade_date, note from future_note_data 
        where main_ts_code = 'common' and trade_date between $2 and $3) a
        order by trade_date, ts_code, freq_code
        """
        args = [main_ts_code, start_date, end_date]
        result_ori = await get_multi_data(self.db_conn, sql, args)
        result = []
        for main_ts_code, ts_code, freq_code, trade_date, note in result_ori:
            result.append({
                "main_ts_code": main_ts_code,
                "ts_code": ts_code,
                "freq_code": freq_code,
                "trade_date": trade_date,
                "note": note
            })

        return result

    async def bs_note_insert(self, main_ts_code, ts_code, freq_code, trade_date, trade_type, number, point, note):
        sql = """
        insert into future_bs_note_data(main_ts_code, ts_code, freq_code, trade_date, trade_type, number, point, note, update_date) values ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        """
        args = [main_ts_code, ts_code, freq_code.value, trade_date, trade_type, number, point, note, datetime.datetime.now()]

        await update_data(self.db_conn, sql, args)

    async def get_bs_note(self, main_ts_code, start_date, end_date):
        sql = """
        select main_ts_code, ts_code, freq_code, trade_date, trade_type, number, point, note from future_bs_note_data 
        where main_ts_code = $1 and trade_date between $2 and $3 order by trade_date, ts_code, freq_code
        """
        args = [main_ts_code, start_date, end_date]
        result_ori = await get_multi_data(self.db_conn, sql, args)
        result = []
        for main_ts_code, ts_code, freq_code, trade_date, trade_type, number, point, note in result_ori:
            result.append({
                "main_ts_code": main_ts_code,
                "ts_code": ts_code,
                "freq_code": freq_code,
                "trade_date": trade_date,
                "trade_type": trade_type,
                "number": number,
                "point": point,
                "note": note
            })

        return result

    async def get_json_data(self, key_name):
        sql = """
        select data::json from json_data
        where name = $1
        """
        args = [key_name]
        result = await get_single_value(self.db_conn, sql, args)
        result = json.loads(result)
        return result

    async def get_strategy_result_data(self, trade_date):
        sql = """
        select a.ts_code, a.main_ts_code, b.name, a.strategy_code, a.freq_code, a.bs_flag from strategy_result a left join
        (select ts_code, name from s_info 
         union
         select ts_code, name from future_basic_info_data) b
         on a.ts_code = b.ts_code
        where a.date=$1 order by a.strategy_code, a.ts_code, a.main_ts_code, a.freq_code, a.bs_flag
        """
        args = [trade_date]
        result_ori = await get_multi_data(self.db_conn, sql, args)
        result = {}
        for ts_code, main_ts_code, name, strategy_code, freq_code, bs_flag in result_ori:
            result.setdefault(strategy_code, []).append({
                "ts_code": ts_code,
                "main_ts_code": main_ts_code,
                "name": name,
                "freq_code": freq_code,
                "bs_flag": bs_flag
            })

        return result
