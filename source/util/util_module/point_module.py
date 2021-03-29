from source.util.util_base.date_util import get_interval_date_list_by_freq_code, adjust_interval_date_list_by_exists_date, adjust_interval_all_date_list_by_exists_date
from source.util.util_data.point_data import PointData


def _get_high_low_point(interval_point_data):
    high_point_list = [i['high'] for i in interval_point_data if i['high'] is not None]
    high = max(high_point_list) if high_point_list else None

    low_point_list = [i['low'] for i in interval_point_data if i['low'] is not None]
    low = min(low_point_list) if low_point_list else None

    return high, low


async def get_main_code_interval_point_data_by_freq_code(db_conn, ts_code, start_date, end_date, freq_code):
    interval_date_list = get_interval_date_list_by_freq_code(start_date, end_date, freq_code)
    interval_point_data = await PointData(db_conn).get_future_interval_point_data_by_main_code(ts_code, interval_date_list[0][0], interval_date_list[-1][-1])
    interval_date_list = adjust_interval_all_date_list_by_exists_date(interval_date_list, list(interval_point_data))

    interval_point_data_by_freq_code = {}
    for date_list in interval_date_list:
        high, low = _get_high_low_point([interval_point_data[i] for i in date_list])
        start_date, end_date = date_list[0], date_list[-1]
        interval_point_data_by_freq_code[end_date] = {
            "ts_code": interval_point_data[end_date]['ts_code'],
            "open": interval_point_data[start_date]['open'],
            "high": high,
            "low": low,
            "close": interval_point_data[end_date]['close'],
            "vol": interval_point_data[end_date]['vol']
        }
    return interval_point_data_by_freq_code


async def get_ts_code_interval_point_data_by_freq_code(db_conn, ts_code, start_date, end_date, freq_code):
    interval_date_list = get_interval_date_list_by_freq_code(start_date, end_date, freq_code)
    interval_point_data = await PointData(db_conn).get_future_interval_point_data(ts_code, interval_date_list[0][0], interval_date_list[-1][-1])
    interval_date_list = adjust_interval_all_date_list_by_exists_date(interval_date_list, list(interval_point_data))

    interval_point_data_by_freq_code = {}
    for date_list in interval_date_list:
        high, low = _get_high_low_point([interval_point_data[i] for i in date_list])
        start_date, end_date = date_list[0], date_list[-1]
        interval_point_data_by_freq_code[end_date] = {
            "ts_code": interval_point_data[end_date]['ts_code'],
            "open": interval_point_data[start_date]['open'],
            "high": high,
            "low": low,
            "close": interval_point_data[end_date]['close'],
            "vol": interval_point_data[end_date]['vol']
        }
    return interval_point_data_by_freq_code


async def get_ts_code_interval_holding_data(db_conn, ts_code, start_date, end_date):
    interval_holding_data_ori = await PointData(db_conn).get_ts_code_interval_holding_data(ts_code, start_date, end_date)

    interval_holding_data = {}
    for date, date_value in interval_holding_data_ori.items():
        interval_holding_data[date] = {"long": [], "short": [], "vol": []}
        for row in date_value:
            if row["broker"] != "期货公司会员":
                if row["long_hld"]:
                    interval_holding_data[date]["long"].append({"broker": row["broker"], "amount": row["long_hld"], "chg": row["long_chg"]})
                if row["short_hld"]:
                    interval_holding_data[date]["short"].append({"broker": row["broker"], "amount": row["short_hld"], "chg": row["short_chg"]})
                if row["vol"]:
                    interval_holding_data[date]["vol"].append({"broker": row["broker"], "amount": row["vol"], "chg": row["vol_chg"]})

    return interval_holding_data
