import datetime

from fastapi import HTTPException, status

from source.util.util_base.db import get_multi_data, get_single_value, get_single_row


class PointData:
    def __init__(self, db_conn):
        self.db_conn = db_conn

    async def get_ts_code_interval_total_holding_data(self, ts_code, start_date, end_date):
        sql = '''
        select ts_code, trade_date, vol, amount, oi from future_daily_point_data where 
        ts_code = $1 and trade_date >= $2 and trade_date <= $3
        order by trade_date
        '''
        args = [ts_code, start_date, end_date]
        result_ori = await get_multi_data(self.db_conn, sql, args)

        result = {}
        for ts_code, trade_date, vol, amount, oi in result_ori:
            result[trade_date] = {
                "ts_code": ts_code,
                "vol": vol,
                "amount": amount,
                "oi": oi
            }

        return result

    async def get_ts_code_interval_holding_data(self, ts_code, start_date, end_date):
        sql = '''
        select trade_date, broker, vol, vol_chg, long_hld, long_chg, short_hld, short_chg from future_holding_data 
        where symbol = $1 and trade_date >= $2 and trade_date <= $3
        order by trade_date
        '''
        args = [ts_code.split(".")[0], start_date, end_date]
        result_ori = await get_multi_data(self.db_conn, sql, args)

        result = {}
        for trade_date, broker, vol, vol_chg, long_hld, long_chg, short_hld, short_chg in result_ori:
            result.setdefault(trade_date, []).append({
                "broker": broker,
                "vol": vol,
                "vol_chg": vol_chg,
                "long_hld": long_hld,
                "long_chg": long_chg,
                "short_hld": short_hld,
                "short_chg": short_chg
            })

        return result

    async def get_future_interval_point_data(self, ts_code, start_date, end_date):
        sql = '''
        select ts_code, trade_date, open, high, low, close, settle, change1, change2, vol, amount from future_daily_point_data where 
        ts_code = $1 and trade_date >= $2 and trade_date <= $3
        order by trade_date
        '''
        args = [ts_code, start_date, end_date]
        result_ori = await get_multi_data(self.db_conn, sql, args)

        result = {}
        for ts_code, trade_date, open_price, high, low, close, settle, change1, change2, vol, amount in result_ori:
            result[trade_date] = {
                "ts_code": ts_code,
                "open": open_price,
                "high": high,
                "low": low,
                "close": close,
                "settle": settle,
                "change1": change1,
                "change2": change2,
                "vol": vol,
                "amount": amount
            }

        return result

    async def get_future_interval_point_data_by_main_code(self, ts_code, start_date, end_date):
        sql = """select a.ts_code, a.trade_date, a.open, a.high, a.low, a.close, a.settle, a.change1, a.change2, a.vol, a.amount from 
        future_daily_point_data a inner join
            (select trade_date, mapping_ts_code from future_main_code_data where ts_code=(
            select ts_code from future_main_code_data where mapping_ts_code= $1 or ts_code=$1 order by length(ts_code) limit 1)) b 
        on a.ts_code=b.mapping_ts_code and a.trade_date=b.trade_date
        where a.trade_date >= $2 and a.trade_date <= $3
        order by a.trade_date"""

        args = [ts_code, start_date, end_date]
        result_ori = await get_multi_data(self.db_conn, sql, args)

        result = {}
        for ts_code, trade_date, open_price, high, low, close, settle, change1, change2, vol, amount in result_ori:
            result[trade_date] = {
                "ts_code": ts_code,
                "open": open_price,
                "high": high,
                "low": low,
                "close": close,
                "settle": settle,
                "change1": change1,
                "change2": change2,
                "vol": vol,
                "amount": amount
            }

        return result

    async def get_future_now_point_by_main_code(self, ts_code, data_date):
        start_date = data_date - datetime.timedelta(days=30)
        sql = """
        select a.close from 
            future_daily_point_data a inner join
                (select trade_date, mapping_ts_code from future_main_code_data where ts_code=(
                select ts_code from future_main_code_data where mapping_ts_code=$1 order by length(ts_code) limit 1)) b 
            on a.ts_code=b.mapping_ts_code and a.trade_date=b.trade_date
            where a.trade_date >= $2 and a.trade_date <= $3
            order by a.trade_date desc
            limit 1
        """
        args = [ts_code, start_date, data_date]
        point_now = await get_single_value(self.db_conn, sql, args)
        if not point_now:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="未获取到点位数据, ts_code={0}, data_date={1}".format(ts_code, data_date)
            )
        return point_now

    async def get_future_max_min_point_by_main_code(self, ts_code, start_date, end_date):
        sql = """
        select max(a.close), min(a.close) from 
                   future_daily_point_data a inner join
                       (select trade_date, mapping_ts_code from future_main_code_data where ts_code=(
                       select ts_code from future_main_code_data where mapping_ts_code=$1 order by length(ts_code) limit 1)) b 
                   on a.ts_code=b.mapping_ts_code and a.trade_date=b.trade_date
                   where a.trade_date >= $2 and a.trade_date <= $3
        """
        args = [ts_code, start_date, end_date]
        result = await get_single_row(self.db_conn, sql, args)
        if not result:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="未获取到最大最小点位数据, ts_code={0}, start_date={1}, end_date={2}".format(ts_code, start_date, end_date)
            )

        max_point, min_point = result
        return max_point, min_point

    async def get_future_max_min_now_point_by_main_code(self, ts_code, start_date, end_date):
        point_now = await self.get_future_now_point_by_main_code(ts_code, end_date)
        max_point, min_point = await self.get_future_max_min_point_by_main_code(ts_code, start_date, end_date)

        return {
            "max_point": max_point,
            "min_point": min_point,
            "point_now": point_now
        }
