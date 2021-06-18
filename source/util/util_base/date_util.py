import calendar
import datetime
from typing import List

import numpy as np
from fastapi import HTTPException, status

from source.util.util_base.constant import FreqCode


def convert_datetime_to_str(date):
    return date.strftime('%Y-%m-%d')


def convert_str_to_datetime(date):
    if isinstance(date, str):
        if "-" in date:
            return datetime.datetime.strptime(date, "%Y-%m-%d")
        else:
            return datetime.datetime.strptime(date, "%Y%m%d")
    else:
        return date


def convert_date_to_datetime(date):
    return datetime.datetime(date.year, date.month, date.day)


def obj_contain_datetime_convert_to_str(obj):
    if isinstance(obj, (list, tuple)):
        new_obj = []
        for value in obj:
            new_obj.append(obj_contain_datetime_convert_to_str(value))
    elif isinstance(obj, dict):
        new_obj = {}
        for key, value in obj.items():
            new_obj[obj_contain_datetime_convert_to_str(key)] = obj_contain_datetime_convert_to_str(value)
    elif isinstance(obj, (datetime.datetime, datetime.date)):
        new_obj = convert_datetime_to_str(obj)
    elif isinstance(obj, (str, int, float, np.float64)):
        new_obj = obj
    elif isinstance(obj, type(None)):
        new_obj = obj
    else:
        raise TypeError("不支持处理的数据类型, type(obj)={0}".format(type(obj)))

    return new_obj


def convert_pd_timestamp_to_datetime(timestamp):
    date = timestamp.date()
    date = datetime.datetime(date.year, date.month, date.day)
    return date


def get_date_range(start_date, end_date):
    date_range = []
    date_now = start_date
    while date_now <= end_date:
        date_range.append(date_now)
        date_now += datetime.timedelta(days=1)

    return date_range


def get_end_date_by_freq_code(data_date, freq_code):
    """
    :param freq_code:  D, W, M
    """
    if freq_code is FreqCode.DAY:
        return data_date
    elif freq_code is FreqCode.WEEK:
        for _ in range(8):
            if data_date.weekday() == 4:
                return data_date
            data_date += datetime.timedelta(days=1)
        else:
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="未获取到周末日期, date={0}, freq_code={1}".format(data_date, freq_code))
    elif freq_code is FreqCode.MONTH:
        _, end_day_number_of_month = calendar.monthrange(data_date.year, data_date.month)
        for _ in range(33):
            if data_date.day == end_day_number_of_month:
                return data_date
            data_date += datetime.timedelta(days=1)
        else:
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="未获取到月末日期, date={0}, freq_code={1}".format(data_date, freq_code))
    else:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="不支持的freq_code={}".format(freq_code))


def get_end_date_list_by_freq_code(start_date, end_date, freq_code):
    """
    :param freq_code:  D, W, M
    """
    end_date = get_end_date_by_freq_code(end_date, freq_code)

    end_date_list = []
    if freq_code is FreqCode.DAY:
        for date_now in get_date_range(start_date, end_date):
            end_date_list.append(date_now)
    elif freq_code is FreqCode.WEEK:
        for date_now in get_date_range(start_date, end_date):
            if date_now.weekday() == 4:
                end_date_list.append(date_now)
    elif freq_code is FreqCode.MONTH:
        for date_now in get_date_range(start_date, end_date):
            _, end_day_number_of_month = calendar.monthrange(date_now.year, date_now.month)
            if date_now.day == end_day_number_of_month:
                end_date_list.append(date_now)
    else:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="不支持的freq_code={}".format(freq_code))

    return end_date_list


def get_interval_date_list_by_freq_code(start_date, end_date, freq_code):
    """
    :param freq_code:  D, W, M
    """
    end_date_list = get_end_date_list_by_freq_code(start_date, end_date, freq_code)

    start_date = start_date
    interval_date_list = []
    for end_date in end_date_list:
        interval_date_list.append([start_date, end_date])
        start_date = end_date + datetime.timedelta(days=1)

    return interval_date_list


def adjust_interval_all_date_list_by_exists_date(interval_date_list: List, date_list: List):
    """
    使用 date_list 调整 interval_date_list 使其中的包含所有date_list中符合条件的日期
    :param interval_date_list:  [[start_date, end_date], ...]
    :param date_list: [date1, date2 ...]
    :return: [[start_date, date_2 ... , end_date], ...]
    """
    if not interval_date_list or not date_list:
        return []

    date_list.sort()
    interval_date_list.sort(key=lambda x: x[0])

    index = 0
    interval_date_list_new = []
    for start_date, end_date in interval_date_list:
        interval_date_list_now = []
        for index_now in range(index, len(date_list)):
            if start_date <= date_list[index_now] <= end_date:
                interval_date_list_now.append(date_list[index_now])
            elif date_list[index_now] < start_date:
                pass
            elif date_list[index_now] > end_date:
                index = index_now
                break

        if interval_date_list_now:
            interval_date_list_new.append(interval_date_list_now)
    return interval_date_list_new


def adjust_interval_date_list_by_exists_date(interval_date_list: List, date_list: List):
    """
    使用 date_list 调整 interval_date_list 使其中的日期在 date_list 中都存在
    :param interval_date_list:  [[start_date, end_date], ...]
    :param date_list: [date1, date2 ...]
    :return: [[start_date, end_date], ...]
    """
    interval_date_list_new = adjust_interval_all_date_list_by_exists_date(interval_date_list, date_list)
    interval_date_list_new = [[i[0], i[-1]] for i in interval_date_list_new]

    return interval_date_list_new


weekdays = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']


def get_previous_byday(dayname, start_date):
    day_num = start_date.weekday()
    day_num_target = weekdays.index(dayname)
    days_ago = (7 + day_num - day_num_target) % 7
    # if days_ago == 0:
    #     days_ago = 7
    target_date = start_date - datetime.timedelta(days=days_ago)
    return target_date


def get_next_byday(dayname, start_date):
    day_num = start_date.weekday()
    day_num_target = weekdays.index(dayname)
    days_ago = (7 + day_num_target - day_num) % 7
    # if days_ago == 0:
    #     days_ago = 7
    target_date = start_date + datetime.timedelta(days=days_ago)
    return target_date
