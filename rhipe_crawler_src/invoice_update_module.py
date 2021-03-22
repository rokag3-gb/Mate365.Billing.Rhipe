import json
from datetime import datetime
import logging as logger

from Common.db_connection import DBConnect
from Common.logger import LOGGER
from Common.tools import get_month_start_and_last_date, datetime_to_json_formatting
from rhipe_crawler_src.prism_controller import PrismController

prism_controller = PrismController()
TIME_FORMAT_NORMAL = "%Y-%m-%dT%H:%M:%S%z"
TIME_FORMAT_RHIPE = "%Y-%m-%dT%H:%M:%S.%f%z"
TIME_FORMAT_INVOICE = "%Y-%m-%dT%H:%M:%S.%f0%z"


def get_invoice_list(t_date: datetime = None):
    invoices = prism_controller.invoice_all()['data']['Records']
    LOGGER.debug(f'Invoice list {invoices}')
    if len(invoices) < 1:
        logger.error('invoice list not exist... Check rhipe api or request params')
        raise ValueError

    if t_date:
        result = []
        for _invoice in invoices:
            _invoice_start_date = datetime.strptime(_invoice['BillingPeriodStart'], TIME_FORMAT_NORMAL)
            if _invoice_start_date.month == t_date.month and _invoice_start_date.year == t_date.year and\
                    _invoice['ProgramName'] == 'Microsoft CSP Indirect':
                result.append(_invoice)
        return result
    return invoices


def check_invoice_list(db: DBConnect, t_date: datetime):
    check = db.select_data(db.get_sql().CHECK_INVOICE_LIST, (t_date.strftime('%Y%m'),))
    LOGGER.debug(f'Invoice Exist Check : {check}')
    return len(check) > 0


def invoice_detail_by_invoiceid(invoice_id: str):
    '''
    invoice_id에 맞는 detail 가져옴.
    :param invoice_id: str
    :return: [details] : list
    '''

    invoice_details = prism_controller.invoice_details(invoice_id)['data']
    logger.debug(invoice_details)
    for d in invoice_details:
        d['ChargeEndDate'] = datetime.strptime(d['ChargeEndDate'], TIME_FORMAT_INVOICE)
        d['ChargeStartDate'] = datetime.strptime(d['ChargeStartDate'], TIME_FORMAT_INVOICE)
    if len(invoice_details) < 1:
        logger.warning("Invoice List가 존재하지않음.")
    # detail들을 subscription 별로 분류.
    # return invoice_classification_by_detail(invoice_details)
    return invoice_details


def select_db_proprecess_day(db: DBConnect, param_date: datetime):
    firstday_date, lastday_date = get_month_start_and_last_date(param_date)
    data = db.select_data(sql=db.get_sql().SELECT_PREPROCESS_OF_DAY_ALL_SQL, data=lastday_date)
    if len(data) < 1:
        logger.error('%s 날짜에 대한 데이터가 없음.' % param_date)
        raise

    for d in data:
        if len(d['body']) > 1:
            d['body'] = json.loads(d['body'])

    return data


def insert_db_invoice_details(db: DBConnect, invoice_id: str, invoice_detail: list):
    input_value = []
    for inv in invoice_detail:
        input_value.append((invoice_id,
                            inv['SubscriptionId'], inv['OfferName'],
                            inv['ChargeStartDate'], inv['ChargeEndDate'],
                            inv['UnitPrice'], inv['UnitPriceRrp'],
                            inv['Quantity'], inv['BillableRatio'],
                            inv['SubTotal'], inv['SubTotalRrp']))

    db.insert_data(sql=db.get_sql().INSERT_INVOICE_SQL, data=input_value)
    logger.info('INVOICE SAVE DONE')


def update_db_preprocess_billing_table(db: DBConnect, date: datetime, invoice_detail: list):
    firstday_date, lastday_date = get_month_start_and_last_date(date)
    invoice_details_by_subscription = invoice_classification_by_detail_to_merge(invoice_detail)

    # invoice details의 정해진 항목을 사용날짜 마지막날에 update.
    # sql 조건은 subscriptionid와 last_update_date로 할것.

    input_value = []

    invoice_key = ['OfferName', 'ChargeStartDate', 'ChargeEndDate', 'UnitPrice', 'UnitPriceRrp',
                   'Quantity', 'BillableRatio', 'SubTotal', 'SubTotalRrp']

    for subscription in invoice_details_by_subscription.values():
        body = {}
        for key in invoice_key:
            body[key] = subscription[key]
        input_value.append((json.dumps(body, default=datetime_to_json_formatting), subscription['SubscriptionId'],
                            firstday_date, lastday_date))

    # db = DBConnect.get_instance()
    db.insert_data(sql=db.get_sql().UPDATE_INVOICE_IN_PREPROCESS_SQL, data=input_value)


def invoice_classification_by_detail_to_merge(invoice_details: list):
    # detail들을 subscription 별로 분류.
    invoice_details_by_subscription = {}
    for detail in invoice_details:
        if detail['SubscriptionId'] == '':
            logger.error('Empty Subscription ID check..\n%s\n' % invoice_details)
            raise ValueError
        if detail['SubscriptionId'] in invoice_details_by_subscription.keys():
            if detail['ChargeEndDate'] > invoice_details_by_subscription[detail['SubscriptionId']]['ChargeEndDate']:
                invoice_details_by_subscription[detail['SubscriptionId']]['Quantity'] = detail['Quantity']
                invoice_details_by_subscription[detail['SubscriptionId']]['ChargeEndDate'] = detail['ChargeEndDate']
            invoice_details_by_subscription[detail['SubscriptionId']]['BillableRatio'] \
                = str(float(detail['BillableRatio']) + float(
                invoice_details_by_subscription[detail['SubscriptionId']]['BillableRatio']))
            invoice_details_by_subscription[detail['SubscriptionId']]['SubTotal'] \
                = str(float(detail['SubTotal']) + float(
                invoice_details_by_subscription[detail['SubscriptionId']]['SubTotal']))
            invoice_details_by_subscription[detail['SubscriptionId']]['SubTotalRrp'] \
                = str(float(detail['SubTotalRrp']) + float(
                invoice_details_by_subscription[detail['SubscriptionId']]['SubTotalRrp']))
            invoice_details_by_subscription[detail['SubscriptionId']]['ChargeStartDate'] \
                = min(detail['ChargeStartDate'],
                      invoice_details_by_subscription[detail['SubscriptionId']]['ChargeStartDate'])
        else:
            invoice_details_by_subscription[detail['SubscriptionId']] = detail

    logger.debug(invoice_details_by_subscription)
    return invoice_details_by_subscription
