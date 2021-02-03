import calendar
import csv
from datetime import date, datetime


def csv_string_to_json(csv_string):
    '''
    :param csv_string: csv 형식의 string
    :return: key value 형태 dict 반환
    '''
    target_split_lines = csv_string.splitlines()
    datas = list(csv.reader(target_split_lines))
    keys = datas[0]
    #
    result = []
    for i in range(1, len(datas)):
        member = {}
        for k, v in zip(keys, datas[i]):
            member[k] = v
        result.append(member)

    return result


def get_month_start_and_last_date(target_datetime: datetime):
    '''
    :param target_datetime: datetime(year, month)
    :return:
    '''
    year = target_datetime.year
    month = target_datetime.month
    now = datetime(year=year, month=month, day=1)
    last = datetime(year=year, month=month, day=calendar.monthrange(year, month)[1])
    return now, last


def datetime_to_json_formatting(o):
    if isinstance(o, (date, datetime)):
        return o.isoformat()
