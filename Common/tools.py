import csv


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