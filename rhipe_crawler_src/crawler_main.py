import json
from datetime import datetime, timedelta
import os
from Common.slack_tool import send_msg_dict, send_text_msg
from rhipe_crawler_src.crawler_module import get_cloudmate_crawl_all_tenant_subscription_list, \
    get_cloudmate_crawl_subscription_summary_detail_combine, TIME_FORMAT_NORMAL, target_last_update_datetime_str, \
    target_last_update_datetime, update_preprocess_to_db, insert_preprocess_to_db, get_customer_info_to_azure_tenant, \
    insert_customer_to_db
from rhipe_crawler_src.envlist import contractagreement_id
from Common.logger import LOGGER
from rhipe_crawler_src.s3_module import upload_to_s3
from Common.db_connection import DBConnect


def crawler(t_date):
    '''

    :param t_date: '%Y-%m-%d'
    :return:
    '''
    if t_date:
        search_date_str = "%sT00:00:00+0000" % t_date
        search_date_datetime = datetime.strptime(search_date_str, TIME_FORMAT_NORMAL)
    else:
        search_date_str = target_last_update_datetime_str()
        search_date_datetime = datetime.strptime(search_date_str, TIME_FORMAT_NORMAL)
    LOGGER.info("[ Rhipe az usage -> CM Database -> GS S3 ] Crawler Start.")
    # TODO: customers_info 호출 후, DB 적재
    azure_customers = get_customer_info_to_azure_tenant()
    insert_customer_to_db(azure_customers)

    tenants = get_cloudmate_crawl_all_tenant_subscription_list(contractagreement_id)
    # print(tenants)
    LOGGER.debug("tenants : %s" % tenants)
    LOGGER.debug("tenant 갯수 : %d" % len(tenants))
    LOGGER.info("Crawling Date : %s " % t_date)
    env = os.getenv('CRAWLER_ENV') or 'dev'

    subscription_info = get_cloudmate_crawl_subscription_summary_detail_combine(tenants=tenants,
                                                                                search_date=search_date_str)
    LOGGER.info("Get Subscription Information : Success")
    len_crawling_data = subscription_info['length']
    # print(subscription_info)
    affected_count = insert_preprocess_to_db(subscription_info)

    s3_upload_amount = 0
    if env == 'prod' or True:
        s3_upload_amount = upload_to_s3(data=subscription_info['subscriptions'],
                                        param_date=search_date_datetime, is_upload=(env == 'prod'))

    # (slack) 완료 노티.
    link_param_date = search_date_datetime.strftime('%Y-%m-%d')
    slack_param = {
        "attachments": [
            {
                "fallback": "Success Daily Crawling - %s" % link_param_date,  # 노티에서 보임
                "color": "#00ffff",  # 청녹?
                "pretext": "[%s] Today Crawler is Done" % datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                "title": "Preprocess Update",
                "title_link": None,
                "text": "Success \ncrawling and update preprocess table, s3 upload",
                "fields": [
                    {
                        "title": "T1 Crawling (Amount)",
                        "value": json.dumps(len_crawling_data, indent=4),
                        "short": True
                    },
                    {
                        "title": "DB INSERT (Amount)",
                        "value": affected_count,
                        "short": True
                    },
                    {
                        "title": "S3 Upload (Amount)",
                        "value": s3_upload_amount,
                        "short": True
                    },

                ],
            }
        ],
        "channel": '#info' if env == 'prod' else '#dev',
        "as_user": True,
        "text": None
    }
    send_msg_dict(slack_param)


def crawler_update(period):
    LOGGER.info("[ Rhipe(T1) Usage -> CM Database -> GS S3 update ] Tool Start.")

    tenants = get_cloudmate_crawl_all_tenant_subscription_list(contractagreement_id)

    start_date_object = target_last_update_datetime()
    end_date_object = start_date_object - timedelta(days=period)
    env = os.getenv('CRAWLER_ENV') or 'dev'
    LOGGER.info("Crawling ENV is [ %s ]" % env)

    db = DBConnect.get_instance()

    send_text_msg(msg='Start Update!',
                  channel='#update_check' if env == 'prod' else '#dev')

    while start_date_object >= end_date_object:
        LOGGER.info('search date : %s' % end_date_object)
        new_data = get_update(tenants=tenants,
                              check_date=end_date_object.strftime(TIME_FORMAT_NORMAL),
                              slack_channel='#update_check' if env == 'prod' else '#dev')

        if update_preprocess_to_db(new_data['subscriptions'], db=db):
            if env == 'prod':
                db.commit()
                send_text_msg(msg='Update 완료',
                              channel='#update_check' if env == 'prod' else '#dev')

        # database select -> s3
        db_date = db.select_data(sql=db.get_sql().SELECT_PREPROCESS_OF_DAY_ALL_SQL,
                                 data=end_date_object)
        if len(db_date) < 1:
            LOGGER.error(f'{end_date_object} 일자의 Database 내용이 존재하지않음.')
            raise
        upload_to_s3(data=db_date,
                     param_date=end_date_object,
                     is_upload=(env == 'prod'))

        end_date_object += timedelta(days=1)

    send_text_msg(msg='Done.',
                  channel='#update_check' if env == 'prod' else '#dev')
    LOGGER.info('Update Done.')


def get_update(tenants, check_date: str, slack_channel='#update_check'):
    # DATE일자 크롤.
    # get_cloudmate_crawl_subscription_summary_detail_combine
    new_data = get_cloudmate_crawl_subscription_summary_detail_combine(tenants, search_date=check_date)

    # REPORT
    report_slack_param = {
        "attachments": [
            {
                "fallback": "Update Check - %s" % check_date,  # 노티에서 보임
                "color": "#00ffff",  # 청녹?
                "title": "[%s] Update Check" % datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                "text": "Check to [%s]" % check_date[:10],
                "fields": [
                    {
                        "title": "T1 Server (Amount)",
                        "value": json.dumps(new_data['length'], indent=4),
                        "short": True
                    },
                ],
            }
        ],
        "channel": slack_channel,
        "as_user": True,
        "text": None
    }
    send_msg_dict(report_slack_param)
    return new_data


#TEST
# if __name__ == '__main__':
#     for i in range(11, 14):
#         t_date = '2020-12-%d' % i
#         crawler(t_date)
