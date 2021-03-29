import datetime

from fastapi import HTTPException, status

from source.util.util_base.db import get_multi_data, get_single_value, get_single_row, update_data, get_single_column


class ManualData:
    def __init__(self, db_conn):
        self.db_conn = db_conn

    async def set_future_manual_note_data(self, root_classify_id, classify_id, classify_name, classify_parent_id, note, note_point_data):
        sql = """
        insert into future_manual_note_data(root_classify_id, classify_id, classify_name, classify_parent_id, note, note_point_data, update_date)
        values (%s, %s, %s, %s, %s, %s, %s)
        """
        args = [root_classify_id, classify_id, classify_name, classify_parent_id, note, note_point_data, datetime.datetime.now()]
        await update_data(self.db_conn, sql, args)

    async def get_future_manual_note_data(self, root_classify_id):
        sql = """
        select root_classify_id, classify_id, classify_name, classify_parent_id, note, note_point_data from future_manual_note_data
        where root_classify_id=%s
        """
        args = [root_classify_id]
        result = await get_multi_data(self.db_conn, sql, args)
        return result

    async def get_future_manual_note_data_root_classify_id(self):
        sql = """
        select distinct root_classify_id, classify_id, classify_name from future_manual_note_data
        where classify_parent_id is null
        """
        result = await get_multi_data(self.db_conn, sql)
        return result

    async def set_set_future_manual_turn_note_data(self, main_ts_code, trade_date, freq_code, point_turn_flag, note):
        sql = """
        insert into future_manual_turn_note_data(main_ts_code, trade_date, freq_code, point_turn_flag, note, update_date)
        values (%s, %s, %s, %s, %s, %s)
        """
        args = [main_ts_code, trade_date, freq_code.value, point_turn_flag, note, datetime.datetime.now()]
        await update_data(self.db_conn, sql, args)

    async def get_set_future_manual_turn_note_data(self, main_ts_code, start_date, end_date, freq_code):
        sql = """
        select main_ts_code, trade_date, freq_code, point_turn_flag, note from future_manual_turn_note_data
        where main_ts_code=%s and trade_date >= %s and trade_date <= %s and freq_code=%s
        """
        args = [main_ts_code, start_date, end_date, freq_code.value]
        result = await get_multi_data(self.db_conn, sql, args)
        return result
