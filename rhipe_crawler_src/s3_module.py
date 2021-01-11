import csv
import json
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
import tempfile
from datetime import datetime

import boto3
import botocore

from Common.slack_tool import file_upload
from Common.logger import LOGGER
from rhipe_crawler_src.crawler_module import TIME_FORMAT_NORMAL
from rhipe_crawler_src.envlist import s3_access_key, s3_secret_key, s3_region_name, s3_bucket_hosts, s3_prefix

TIME_FORMAT_CSV = "%Y-%m-%d %H:%M:%S"


s3 = boto3.resource(service_name='s3', aws_access_key_id=s3_access_key,
                    aws_secret_access_key=s3_secret_key, region_name=s3_region_name)
bk_list = []

s3_bucket_hosts_list = s3_bucket_hosts.split(' ')

if not len(s3_bucket_hosts_list) > 0:
    LOGGER.error('input s3 buckect name...')
    raise ValueError

for host in s3_bucket_hosts_list:
    bk_list.append(s3.Bucket(host))


def s3_check_connect():
    for _host in s3_bucket_hosts_list:
        try:
            c = s3.meta.client.head_bucket(Bucket=_host)
            if c['ResponseMetadata']['HTTPStatusCode'] != 200:
                raise ConnectionRefusedError
        except botocore.exceptions.ClientError as e:
            LOGGER.error('S3 Client Error : %s' % e)
            raise


def s3_upload_all_bucket(filename, key):
    for bk in bk_list:
        bk.upload_file(Filename=filename, Key=s3_prefix + key)
        LOGGER.info('Upload to - %s' % s3_prefix + key)

    return len(bk_list)


def s3_show_objects_all_bucket():
    for bk in bk_list:
        LOGGER.info('-----------------------%s-------------------------' % bk)
        for f in bk.objects.all():
            LOGGER.info(f.key)
        LOGGER.info('-----------------------------------------------------------------')


def upload_to_s3(data, param_date: datetime, is_upload=True):
    '''

    :param param_date: string "%Y-%m-%d"
    :param is_upload: s3 업로드 여부. (test용)
    :return:
    '''
    upload_amount = 0
    target_date = param_date.strftime("%Y-%m-%d")
    temp_file_path = tempfile.gettempdir()
    csv_path = os.path.join(temp_file_path, '%s.csv' % target_date)
    with open(csv_path, 'w', newline='') as csv_file:
        fields = ['value']
        writer = csv.DictWriter(csv_file, fieldnames=fields, delimiter='\t', quoting=csv.QUOTE_NONE, quotechar="'")
        writer.writeheader()

        for s in data:
            d = json.dumps({'tenant': s['tenant'],
                            'subscription': s['subscription'],
                            'body': s['body'] if isinstance(s['body'], dict) else json.loads(s['body']),
                            'last_update_date': datetime.strptime(s['last_update_date'], TIME_FORMAT_NORMAL).strftime(TIME_FORMAT_CSV) if not isinstance(s['last_update_date'], datetime) else s['last_update_date'].strftime(TIME_FORMAT_CSV)
                            })
            writer.writerow(dict(value=d))

    # key
    # 2019/06/20190616.csv
    s3_check_connect()
    if is_upload:
        upload_amount = s3_upload_all_bucket(filename=csv_path,
                                             key='%d/%.2d/%d%.2d%.2d.csv' % (param_date.year,
                                                                             param_date.month,
                                                                             param_date.year,
                                                                             param_date.month,
                                                                             param_date.day))
    else:
        file_upload(csv_path, "#dev")

    LOGGER.info('S3 Upload Done.')
    return upload_amount
