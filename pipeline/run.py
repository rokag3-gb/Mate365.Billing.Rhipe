"""
Azure Devops 파이프라인에서 시간에 맞춰 크롤링.

"""
import argparse
import logging
import os
from rhipe_crawler_src.crawler_main import crawler, crawler_update

description = "Azure 빌링 Rhipe 크롤러"
parser = argparse.ArgumentParser(description=description)

parser.add_argument('-t', '--type', type=str, required=False, default='crawler')
parser.add_argument('-d', '--date', type=str, required=False, default=None)
parser.add_argument('-p', '--period', type=int, required=False, default=int(os.environ['CRAWLER_UPDATE_PERIOD']))
args = parser.parse_args()

if args.type == 'crawler':
    crawler(args.date)
elif args.type == 'update':
    crawler_update(args.period)
else:
    logging.error('arguments 확인')
    exit(-1)