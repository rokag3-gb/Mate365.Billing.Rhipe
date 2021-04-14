from datetime import datetime, timedelta
import os
from Common.teams_msg import send_teams_msg
from rhipe_crawler_src.crawler_module import get_cloudmate_crawl_all_tenant_subscription_list, \
    get_cloudmate_crawl_subscription_summary_detail_combine, TIME_FORMAT_NORMAL, target_last_update_datetime_str, \
    target_last_update_datetime, update_preprocess_to_db, insert_preprocess_to_db, get_customer_info_to_azure_tenant, \
    insert_customer_to_db, get_all_csp_price_table_from_rhipe, insert_rhipe_price_table_to_cm, \
    delete_all_csp_price_table_from_cm
from rhipe_crawler_src.envlist import contractagreement_id
from Common.logger import LOGGER
from rhipe_crawler_src.invoice_update_module import get_invoice_list, check_invoice_list, invoice_detail_by_invoiceid, \
    insert_db_invoice_details
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

    # customers_info 호출 후, DB 적재
    azure_customers = get_customer_info_to_azure_tenant(include_deactivated_customers=True)
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
    if os.environ['S3_ENABLE'] == 'enable':
        s3_upload_amount = upload_to_s3(data=subscription_info['subscriptions'],
                                        param_date=search_date_datetime, is_upload=(env == 'prod'))

    # (Teams) 완료 노티.
    msg = f'Rhipe Resource mount: {len_crawling_data}\nDone.'
    send_teams_msg(msg)


def crawler_update(period):
    LOGGER.info("[ Rhipe(T1) Usage -> CM Database -> GS S3 update ] Tool Start.")

    tenants = get_cloudmate_crawl_all_tenant_subscription_list(contractagreement_id)

    start_date_object = target_last_update_datetime()
    end_date_object = start_date_object - timedelta(days=period)
    env = os.getenv('CRAWLER_ENV') or 'dev'
    LOGGER.info("Crawling ENV is [ %s ]" % env)

    db = DBConnect.get_instance()

    # (Teams) start 완료 노티.
    msg = f'Start Update! RUN ENV:{env}'
    send_teams_msg(msg)

    while start_date_object >= end_date_object:
        LOGGER.info('search date : %s' % end_date_object)
        new_data = get_update(tenants=tenants,
                              check_date=end_date_object.strftime(TIME_FORMAT_NORMAL),
                              slack_channel='#update_check' if env == 'prod' else '#dev')

        if update_preprocess_to_db(new_data['subscriptions'], db=db):
            if env == 'prod':
                db.commit()
                msg = f'Finish Update! search {end_date_object} (RUN ENV:{env})'
                send_teams_msg(msg)

        # database select -> s3
        db_date = db.select_data(sql=db.get_sql().SELECT_PREPROCESS_OF_DAY_ALL_SQL,
                                 data=end_date_object)
        if len(db_date) < 1:
            LOGGER.error(f'{end_date_object} 일자의 Database 내용이 존재하지않음.')
            raise
        if os.environ['S3_ENABLE'] == 'enable':
            upload_to_s3(data=db_date,
                         param_date=end_date_object,
                         is_upload=(env == 'prod'))

        end_date_object += timedelta(days=1)

    msg = f'ALL Finish Update! RUN ENV:{env}'
    send_teams_msg(msg)
    LOGGER.info('Update Done.')


def get_update(tenants, check_date: str, slack_channel='#update_check'):
    # DATE일자 크롤.
    # get_cloudmate_crawl_subscription_summary_detail_combine
    new_data = get_cloudmate_crawl_subscription_summary_detail_combine(tenants, search_date=check_date)

    return new_data


def price_table_update():
    price_table = get_all_csp_price_table_from_rhipe(contractagreement_id=contractagreement_id)
    delete_all_csp_price_table_from_cm()
    insert_rhipe_price_table_to_cm(price_table=price_table)
    DBConnect.get_instance().commit()


def invoice_crawler(t_date: datetime = None):
    # S3 제외.
    print('[INFO] CM Invoice Crawling Manager.')

    if t_date is None:
        t_date = datetime.now()

    # DB에 해당 invoice 존재 파악
    if check_invoice_list(DBConnect.get_instance(), t_date):
        LOGGER.error(f'{t_date} Invoice 존재. Exit.')
        return

    invoice_list = get_invoice_list(t_date)

    if len(invoice_list) < 1:
        LOGGER.error(f'{t_date} Rhipe Invoice 존재X. 확인필요.')
        return

    if len(invoice_list) > 1:
        LOGGER.error(f'{t_date} Rhipe Invoice 여러개 존재. 확인필요.\n {invoice_list}')
        return

    target_invoice = invoice_list[0]
    invoice_id = target_invoice['InvoiceId']

    details = invoice_detail_by_invoiceid(invoice_id)
    insert_db_invoice_details(DBConnect.get_instance(), invoice_id, details)
    DBConnect.get_instance().commit()


#TEST
# if __name__ == '__main__':
#     for i in range(11, 14):
#         t_date = '2020-12-%d' % i
#         crawler(t_date)
