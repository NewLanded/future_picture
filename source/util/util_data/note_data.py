import datetime

from source.util.util_base.db import get_multi_data, update_data
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
        select main_ts_code, ts_code, freq_code, trade_date, note from future_note_data 
        where main_ts_code = $1 and trade_date between $2 and $3 order by trade_date, ts_code, freq_code
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

