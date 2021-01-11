import json
import sys
import os

from Common.db_sql import SELECT_PRODUCT_PRICE_ALL

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from datetime import timedelta, timezone, datetime

from Common.db_connection import DBConnect
from Common.logger import LOGGER
from rhipe_crawler_src.data.data_to_json import tenant_json, services_json, detail_json, summary_json
from rhipe_crawler_src.prism_controller import PrismController

prism_controller = PrismController()
TIME_FORMAT_NORMAL = "%Y-%m-%dT%H:%M:%S%z"
TIME_FORMAT_RHIPE = "%Y-%m-%dT%H:%M:%S.%f%z"
TIME_FORMAT_INVOICE = "%Y-%m-%dT%H:%M:%S.%f0%z"


def get_cloudmate_crawl_subscription_summary_detail_combine(tenants, search_date=None, aggregation="ByResourceCategoryAndDay"):
    '''
    해당 tenant의 subscription summary 정보 호출, Detail도 같이 호출해서 합침.
    CM entity에 맞게 변환 후 리턴.,
    :param tenants:
    :param search_date: str
    :param aggregation:
    :return:
    '''
    if search_date is None:
        start_date, end_date = get_crawl_time_from_today()
    else:
        start_date = search_date
        end_date = datetime.strptime(start_date, TIME_FORMAT_NORMAL) + timedelta(days=1) - timedelta(seconds=1)
        end_date = end_date.strftime(TIME_FORMAT_NORMAL)

    detail_len = 0
    subscriptions_combine = []
    for tenant in tenants:
        for subscription in tenant['data']['Subscriptions']:
            # 구매일자가 검색일자 이후 일때, pass
            try:
                purchased_date = datetime.strptime(subscription['FirstPurchased'], TIME_FORMAT_RHIPE)
            except ValueError:
                purchased_date = datetime.strptime(subscription['FirstPurchased'], TIME_FORMAT_NORMAL)
            # if datetime.strptime(subscription['FirstPurchased'], TIME_FORMAT_RHIPE) > datetime.strptime(end_date, TIME_FORMAT_NORMAL):
            if purchased_date > datetime.strptime(end_date, TIME_FORMAT_NORMAL):
                continue

            if subscription['DisplayTemplate'] == "Azure":
                service_resp_summary = prism_controller.subscription_usage_summary(subscription_id=subscription['SubscriptionId'],
                                                                                   start_date=start_date,
                                                                                   end_date=end_date,
                                                                                   aggregation=aggregation)
                services = services_json(service_resp_summary['data'])
                service_resp_detail = prism_controller.subscription_usage_detail(subscription_id=subscription['SubscriptionId'],
                                                                                 start_date=start_date,
                                                                                 end_date=end_date)

                if len(service_resp_detail['data']['UsageLineItems']) > 0:
                    subscription_detail_entity = detail_json({'tenant': tenant['TenantId'],
                                                              'subscription': subscription['SubscriptionId'],
                                                              'last_update_date': start_date,
                                                              'body': service_resp_detail['data']['UsageLineItems']})
                    detail_len += len(service_resp_detail['data']['UsageLineItems'])
                    services['ResourceUsageDetails'] = subscription_detail_entity['body']

                subscription['ProductName'] = 'Azure'
                subscription['Services'] = services
            else:
                # office 제품군 일때
                billingtable = get_csp_price_table(product_id=subscription['ProductId'])
                if billingtable is None:
                    LOGGER.error(
                        '[ERROR] 맞는 제품이 없습니다. Product ID : %s \n subscription : %s' % (subscription['ProductId'],
                                                                                       subscription))
                    continue
                subscription['ProductName'] = billingtable['product_name']
                # subscription['BillingTable'] = BillingTable_data(**billingtable)

            subscriptions_combine.append(summary_json({'subscription': subscription['SubscriptionId'],
                                                       'tenant': tenant['TenantId'],
                                                       'last_update_date': start_date,
                                                       'body': subscription}))

    return {'subscriptions': subscriptions_combine,
            'length': {
                'detail': detail_len,
                'subscription': len(subscriptions_combine)
            }
            }


def get_cloudmate_crawl_all_tenant_subscription_list(contractagreement_id):

    '''
    tenants, subscription 수집
     ex) [
            { tanentid: str,
              ...
              subscriptions: [...],
              ...
            }
         ]

    '''

    # 1
    resp = prism_controller.tenants_subscriptions_info(contract_agreement_id=contractagreement_id)
    tenants = []
    LOGGER.debug('tenants_subscriptions_info 결과 : %s' % resp)
    for tenant_info in resp["data"]:
        tenants.append({'TenantId': tenant_info['TenantId'], 'data': tenant_json(tenant_info)})

    return tenants


def insert_preprocess_to_db(subscription_info: dict):
    input_value = []
    db = DBConnect.get_instance()
    for s in subscription_info['subscriptions']:
        _last_update_date = datetime.strptime(s['last_update_date'], TIME_FORMAT_NORMAL)
        input_value.append(
            (s['tenant'], s['subscription'], json.dumps(s['body']), _last_update_date))
        if len(db.select_data(sql=db.get_sql().SELECT_PREPROCESS_OF_DAY_AND_SUBSCRIPTION_SQL,
                              data=(_last_update_date, s['subscription']))):
            LOGGER.warning('[WARNING] DELETE! Before insert data. %s %s' % (s['subscription'], _last_update_date))
            db.delete_data(sql=db.get_sql().DELETE_PREPROCESS_OF_DAY_SQL,
                           data=(_last_update_date, s['subscription']))

    # TODO: delete 기존 값 을 했을때, 값이있으면 warning.
    affected_count = db.insert_data(sql=db.get_sql().INSERT_PREPROCESS_SQL, data=input_value)

    db.commit()
    LOGGER.info("CM [%s] DB SAVE SUCCESS" % os.environ['CRAWLER_ENV'])

    return affected_count


def update_preprocess_to_db(data: list, db: DBConnect):
    # len_data = len(data)
    input_value = []
    for subscription in data:
        services = subscription['body']['Services']
        if services is None:
            services_t = None
        else:
            services_t = json.dumps(services)
        input_value.append((services_t,
                            subscription['tenant'],
                            subscription['subscription'],
                            datetime.strptime(subscription['last_update_date'], TIME_FORMAT_NORMAL)))

    tran_len = db.insert_data(sql=db.get_sql().UPDATE_PREPROCESS_SQL,
                              data=input_value)

    return tran_len


price_table = None


def get_price_table():
    global price_table

    if price_table is not None:
        return price_table

    db = DBConnect()
    sql = SELECT_PRODUCT_PRICE_ALL
    return db.select_data(sql=sql)


def get_csp_price_table(product_id):
    # TODO: DB에서 price table 가져오기.
    for t in get_price_table():
        if product_id == t['product_id_rhipe'] or product_id == t['product_id_SKU']:
            return dict(partner_price=t['partner_price'],
                        retail_price=t['retail_price'],
                        retail_unit_price=t['retail_unit_price'],
                        product_name=t['product_name'])

    return None


def get_crawl_time_from_today():
    '''
    현재시간기준, 최신 prism Crawling 시간을 반환.
    str 타입.
    :return: start_date, end_date
    '''
    start_date = target_last_update_datetime()
    end_date = start_date + timedelta(days=1) - timedelta(seconds=1)
    return start_date.strftime(TIME_FORMAT_NORMAL), end_date.strftime(TIME_FORMAT_NORMAL)


def target_last_update_datetime():
    '''
    현재시간 기준, prism의 마지막 업데이트시간을 계산.
    datetime 타입형태로 반환.
    :return:
    '''
    return pst_time_one_day_ago().replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)


def pst_time_one_day_ago():
    '''
    PST 시간에서 하루전 시간.
    :return:
    '''
    return datetime.utcnow().replace(tzinfo=timezone(timedelta(hours=-8))) - timedelta(days=1, hours=8)


def target_last_update_datetime_str():
    '''
    현재시간 기준, prism의 마지막 업데이트시간을 계산.
    str 형태로 반환
    :return:
    '''
    return target_last_update_datetime().strftime(TIME_FORMAT_NORMAL)
