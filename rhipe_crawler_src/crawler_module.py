import json
import sys
import os

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
                while True:
                    cost_sum = 0
                    for item in service_resp_detail['data']['UsageLineItems']:
                        cost_sum += item['Cost']
                    total_cost = services['TotalCost']
                    if round(float(total_cost), 5) != round(cost_sum, 5):
                        LOGGER.info(f'Total Cost가 달라 다시 요청 Total Cost : {total_cost}, item 합계 : {cost_sum}')
                        service_resp_detail = prism_controller.subscription_usage_detail(
                            subscription_id=subscription['SubscriptionId'],
                            start_date=start_date,
                            end_date=end_date)
                    else:
                        break

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
                billingtable = get_csp_price_table_from_cm(product_id=subscription['ProductId'])
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


def get_customer_info_to_azure_tenant(include_deactivated_customers=False) -> list:
    """

    :return: {'tenant': customer, ...}
    """
    resp = prism_controller.customers_info(include_deactivated_customers=include_deactivated_customers)['data']
    result = []
    for customer in resp:
        for tenant in customer['CustomerProgramAgreements']:
            _customer_info = customer.copy()
            _customer_info.pop('CustomerProgramAgreements', None)
            _customer_info.update(tenant)
            _customer_info['TenantId'] = _customer_info['Id']
            result.append(_customer_info)
    LOGGER.debug(f'Customer Azure info : {result}')
    return result


def insert_customer_to_db(customer_list: list):
    db = DBConnect.get_instance()
    sql = db.get_sql().INSERT_CUSTOMER
    insert_data = []
    for tenant in customer_list:
        # [CustomerId], [CrmAccountId], [IsPartnerCustomer], [CustomerName], [CustomerNotificationEmail], [ParentCustomerId],
        # [RegistrationNumber], [SignedWithRhipe], [WebUrl], [MainPhone], [Fax], [Street1], [Street2], [Street3], [City],
        # [State], [Postcode], [Country], [CountryIsoCode], [CrmId], [FinanceAccountId], [FinanceAccounts], [DirectDebitWholeAccount],
        # [Email], [BillingStreet1], [BillingStreet2], [BillingStreet3], [BillingCity], [BillingState], [BillingPostcode],
        # [BillingCountry], [BillingCountryIsoCode], [SalesTerritoryName], [SalesPersonFirstName], [SalesPersonLastName],
        # [AccountManagerFirstName], [AccountManagerLastName], [HowDidYouHearAboutRhipe], [HowDidYouHearAboutRhipeOther],
        # [IndustryType], [IndustryTypeOther], [TenantId], [ProgramId], [AgreementStartDate], [AgreementEndDate], [ContractAgreementId],
        # [BillingPeriod], [ProgramReferenceId], [ProgramReferenceLabel], [ProgramName], [Customer], [IsConsumptionProgram], [Contacts],
        # [CreditCard], [PaymentMethodDetails], [HasContractAgreement], [IsActive], [ReferringPartnerName], [IsRhipeEndCustomer],
        # [IsRhipePartnerCustomer], [RegDate]
        insert_data.append((tenant['CustomerId'], tenant['CrmAccountId'], tenant['IsPartnerCustomer'], tenant['CustomerName'],
                            tenant['CustomerNotificationEmail'], tenant['ParentCustomerId'], tenant['RegistrationNumber'], tenant['SignedWithRhipe'],
                            tenant['WebUrl'], tenant['MainPhone'], tenant['Fax'], tenant['Street1'],
                            tenant['Street2'], tenant['Street3'], tenant['City'], tenant['State'],
                            tenant['Postcode'], tenant['Country'], tenant['CountryIsoCode'], tenant['CrmId'],
                            tenant['FinanceAccountId'], json.dumps(tenant['FinanceAccounts']), tenant['DirectDebitWholeAccount'], tenant['Email'],
                            tenant['BillingStreet1'], tenant['BillingStreet2'], tenant['BillingStreet3'], tenant['BillingCity'],
                            tenant['BillingState'], tenant['BillingPostcode'], tenant['BillingCountry'], tenant['BillingCountryIsoCode'],
                            tenant['SalesTerritoryName'], tenant['SalesPersonFirstName'], tenant['SalesPersonLastName'], tenant['AccountManagerFirstName'],
                            tenant['AccountManagerLastName'], tenant['HowDidYouHearAboutRhipe'],
                            tenant['HowDidYouHearAboutRhipeOther'], tenant['IndustryType'],
                            tenant['IndustryTypeOther'], tenant['TenantId'], tenant['ProgramId'], tenant['AgreementStartDate'],
                            tenant['AgreementEndDate'], tenant['ContractAgreementId'], tenant['BillingPeriod'], tenant['ProgramReferenceId'],
                            tenant['ProgramReferenceLabel'], tenant['ProgramName'], tenant['Customer'], tenant['IsConsumptionProgram'],
                            json.dumps(tenant['Contacts']), tenant['CreditCard'], json.dumps(tenant['PaymentMethodDetails']), tenant['HasContractAgreement'],
                            tenant['IsActive'], tenant['ReferringPartnerName'], tenant['IsRhipeEndCustomer'],
                            tenant['IsRhipePartnerCustomer'], datetime.now()))
    db.insert_data(sql=sql, data=insert_data)


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

    db = DBConnect.get_instance()
    sql = db.get_sql().SELECT_PRODUCT_PRICE_ALL
    return db.select_data(sql=sql)


def get_csp_price_table_from_cm(product_id):
    # TODO: DB에서 price table 가져오기.
    for t in get_price_table():
        if product_id == t['product_id_rhipe'] or product_id == t['product_id_SKU']:
            return dict(partner_price=t['partner_price'],
                        retail_price=t['retail_price'],
                        retail_unit_price=t['retail_unit_price'],
                        product_name=t['product_name'])

    return None


def get_all_csp_price_table_from_rhipe(contractagreement_id):
    price_table_json = prism_controller.csp_pricelist(contract_agreement_id=contractagreement_id)['data']
    price_table = []
    for group in price_table_json['ProductGroups']:
        for product in group['Products']:
            price_table.append(product)
    return price_table


def delete_all_csp_price_table_from_cm():
    db = DBConnect.get_instance()
    sql = db.get_sql().DELETE_RHIPE_PRODUCT_PRICE
    db.delete_data(sql)


def insert_rhipe_price_table_to_cm(price_table):
    """

    :param price_table: [{'ProductId': '778a4dce-0014-4d53-8647-314ef2b091d2', 'ProductName': 'Microsoft 365 A1', 'ProductGroup': 'Microsoft 365', 'ProductGroupId': 'c5d61a8e-5c13-47b8-9a61-9bebfd0a3d86', 'ProductSku': '778A4DCE-0014-4D53-8647-314EF2B091D2', 'Price': 352.0, 'MinQty': 1, 'MaxQty': 10000000, 'Qty': 1, 'UnitPrice': 352.0, 'ProductUnit': 'Licenses', 'ProductFrequencies': ['6 Years'], 'RetailPrice': 391.0, 'RetailUnitPrice': 391.0, 'CommitmentValue': None, 'TieredProductSku': None, 'ProductType': 'NON-SPECIFIC', 'ProductDescription': 'A simple offer for lightweight management and productivity for education devices. This is a one-time purchased for six years with no cancellation.', 'ProductShortDescription': None, 'ProductRestrictions': None, 'IsTrialProduct': False, 'BillingCycleOptions': 2, 'BillingCycleDuration': 6, 'CommitmentValueUnitPrice': None},
    :return:
    """
    db = DBConnect.get_instance()
    sql = db.get_sql().INSERT_RHIPE_PRODUCT_PRICE
    insert_data = []
    # [product_name], [product_id_SKU], [product_id_rhipe], [partner_price], [retail_price], [retail_unit_price], [datetime_stamp]
    for price in price_table:
        insert_data.append((price['ProductName'], price['ProductSku'], price['ProductId'], price['Price'], price['RetailPrice'],
                            float(price['RetailPrice'])/100, datetime.now()))
    db.insert_data(sql=sql, data=insert_data)


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
