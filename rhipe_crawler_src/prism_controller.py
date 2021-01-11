import urllib.parse

import requests

from Common.tools import csv_string_to_json
from Common.logger import LOGGER
import time

from rhipe_crawler_src.envlist import client_id, client_secret


class PrismController:

    CUSTOMERS_INFO_ENDPOINT = "/api/v1/me/customers"
    TENANTS_INFO_ENDPOINT = "/api/v2/microsoftcsp/tenants"
    SUBSCRIPTIONS_INFO_ENDPOINT = "/api/v2/microsoftcsp/subscriptions/{subscriptionid}"
    TENANTS_SUBSCRIPTIONS_INFO_ENDPOINT = "/api/v2/contractagreements/{contractAgreementId}/tenants"
    INVOICE_ALL_ENDPOINT = "/api/v2/invoices"
    INVOICE_DETAIL_ENDPOINT = "/api/v1/me/invoice/{invoiceId}/csv"
    MY_INFORMATION = "/api/v1/me"
    SUBSCRIPTIONS_USAGE = "/api/v2/microsoftcsp/azure/usage/summary/{subscription_id}?startDate={start_date}&endDate={end_date}&aggregation={aggregation}"
    SUBSCRIPTIONS_USAGE_DETAIL = "/api/v2/microsoftcsp/azure/detailed/{subscription_id}"

    def __init__(self):

        self._api_caller = PrismApiCaller()

    def customers_info(self):
        '''

        :return:
        '''
        param = {"endpoint": self.CUSTOMERS_INFO_ENDPOINT,
                 "method": "GET",
                 "param": {},
                 "body": {}
                 }
        LOGGER.debug(param)
        return self._api_caller.request(param)

    def tenants_info(self, tenantid=None):
        '''

        :param tenantid:
        :return:
        '''
        endpoint = self.TENANTS_INFO_ENDPOINT
        if tenantid is not None:
            endpoint += "/" + tenantid
        param = {"endpoint": endpoint,
                 "method": "GET",
                 "param": {},
                 "body": {}
                 }
        LOGGER.debug(param)
        return self._api_caller.request(param)

    def subscription_info(self, subscription_id):
        '''

        :param subscription_id:
        :return:
        '''
        if subscription_id is None or subscription_id == "":
            LOGGER.error("subscription is %s" % subscription_id)
            raise ValueError

        param = {"endpoint": self.SUBSCRIPTIONS_INFO_ENDPOINT.format(subscriptionid=subscription_id),
                 "method": "GET",
                 "param": {},
                 "body": {}
                 }
        LOGGER.debug(param)
        return self._api_caller.request(param)

    def tenants_subscriptions_info(self, contract_agreement_id):
        '''

        :param contract_agreement_id:
        :return:
        '''
        param = {
            "endpoint": self.TENANTS_SUBSCRIPTIONS_INFO_ENDPOINT,
            "method": "GET",
            "param": {},
            "body": {},
            "contractAgreementId": contract_agreement_id,
            "comment": "Getting a List Of Tenants and their Associated Subscriptions "
        }
        LOGGER.debug(param)
        return self._api_caller.request(param)

    def subscription_usage_summary(self, subscription_id, start_date, end_date, aggregation="ByResourceOnly"):
        '''

        :param subscription_id:
        :param start_date: Must have Timeoffset
        :param end_date: Must have Timeoffset
        :param aggregation : ByResourceOnly ByResourceCategoryAndDay ByResourceAndDay
        :return:
        '''
        param = {
            "endpoint": self.SUBSCRIPTIONS_USAGE,
            "method": "GET",
            "param": None,
            "subscription_id": subscription_id,
            "start_date": urllib.parse.quote_plus(start_date),
            "end_date": urllib.parse.quote_plus(end_date),
            "aggregation": aggregation,
            "body": None,
            "comment": "Getting a List Of Tenants and their Associated Subscriptions "
        }
        LOGGER.debug(param)
        return self._api_caller.request(param)

    def invoice_all(self):
        '''

        :return:
        '''
        param = {"endpoint": self.INVOICE_ALL_ENDPOINT,
                 "method": "GET",
                 "param": {},
                 "body": {}
                 }
        LOGGER.debug(param)
        return self._api_caller.request(param)

    def invoice_details(self, invoice_id):
        '''

        :param invoice_id:
        :return:
        '''
        param = {"endpoint": self.INVOICE_DETAIL_ENDPOINT,
                 "method": "GET",
                 "invoiceId": invoice_id,
                 "param": {},
                 "body": {}
                 }
        LOGGER.debug(param)
        return self._api_caller.request(param, response_type='csv')

    def my_info(self):
        '''

        :return:
        '''
        param = {"endpoint": self.MY_INFORMATION,
                 "method": "GET",
                 "param": {},
                 "body": {}
                 }
        LOGGER.debug(param)
        return self._api_caller.request(param)

    def subscription_usage_detail(self, subscription_id, start_date, end_date, page_size=500):
        '''

        :param subscription_id:
        :param start_date: "2019-05-28T00:00:00.000Z"
        :param end_date: "2019-05-28T23:59:59.000Z"
        :param page_size:
        :return:
        '''
        param = {
            "endpoint": self.SUBSCRIPTIONS_USAGE_DETAIL,
            "method": "POST",
            "param": {},
            "subscription_id": subscription_id,
            "body": {"startDate": start_date,
                     "endDate": end_date,
                     "pageSize": page_size}
        }
        LOGGER.debug(param)
        return self._api_caller.request(param)


class PrismApiCaller:

    PRISM_BASE_URL = "https://api.prismportal.online"

    def __init__(self, timeout=60):

        self.timeout = timeout
        self.token_service = PrismTokenService()

    def request(self, request_param, response_type='json'):

        self.token_service.token_refresh()

        result = None
        for i in range(5):
            try:
                result = self._api_call(request_param, response_type)
            except:
                LOGGER.warning("Try again")
                LOGGER.exception('exception message')
                continue
            break

        if result is None:
            LOGGER.error('Fail request - No have response data ')
        return result

    def _api_call(self, request_param, response_type='json'):
        '''

        :param request_param:{"endpoint": string, "method": string, "param": dict, "body": dict, ...etc}
        :return: success(bool)
        '''

        result = None

        header = {
            'Authorization': 'Bearer %s' % self.token_service.access_token,
            'Content-type': 'application/json',
            'charset': 'utf-8'
        }
        url = None
        if '{' in request_param['endpoint']:
            url = self.PRISM_BASE_URL + request_param['endpoint'].format(**request_param)
        else:
            url = self.PRISM_BASE_URL + request_param['endpoint']

        req = None
        if request_param["method"] == "POST":
            req = requests.post(url, headers=header, json=request_param['body'])
        elif request_param["method"] == "GET":
            req = requests.get(url, headers=header, params=request_param['param'])
        else:
            raise

        start_time = time.time()

        LOGGER.info("Request - %s" % url)
        # curl_handler.perform()
        respond_code = req.status_code
        if respond_code == 200:
            if response_type == 'json':
                result = req.json()
            elif response_type == 'csv':
                result = csv_string_to_json(req.text)

        elif respond_code == 500 or respond_code == 404:
            LOGGER.error('CODE :: %d' % respond_code)
            LOGGER.error(req.content)
            LOGGER.error(req.text)
        else:
            LOGGER.error('CODE :: %d' % respond_code)
            LOGGER.error(req.text)
            req.raise_for_status()

        # curl_handler.close()
        req.close()
        LOGGER.info("Respone time : %s s" % (time.time() - start_time))

        return {"data": result, "status": respond_code}


class PrismTokenService:

    PRISM_LOGIN_URL = "https://identity.prismportal.online/core/connect/token"

    def __init__(self):
        self.__token_info = None
        self.__login_post_data = {
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": "rhipeapi"
        }
        self.__token_info = self._login()
        LOGGER.debug('prism login data : %s' % self.__login_post_data)

    @property
    def access_token(self):
        if self.__token_info is None:
            LOGGER.error("Access Token not exist....")
            raise ValueError
        return self.__token_info['access_token']

    def token_refresh(self):
        for i in range(5):
            try:
                self.__token_info = self._login()
            except:
                LOGGER.warning('[Login] try again')
                continue
            break

    def _login(self, force=False):

        if self._token_refresh_check():
            return self.__token_info

        req = requests.post(self.PRISM_LOGIN_URL, data=self.__login_post_data)

        req.raise_for_status()

        json_result = req.json()
        if 'access_token' in json_result:
            access_token = json_result
            access_token['update_at'] = time.time()
        else:
            LOGGER.error("Not Contain access_token... Check!\n%s" % json_result)
            raise
        LOGGER.info('New token')
        return access_token

    def _token_refresh_check(self):
        '''
        토큰이 사용가능한지 체크.
        :return: 사용가능 True, 그 외 False
        '''
        # 아무값없을때
        if self.__token_info is None:
            return False

        # 시간 계산
        expires_time = self.__token_info['update_at'] + self.__token_info['expires_in']
        now_time = time.time()
        LOGGER.debug('update_at : %s' % self.__token_info['update_at'])
        LOGGER.debug('expires_in : %s' % self.__token_info['expires_in'])
        LOGGER.debug('now_time : %s' % now_time)
        if (expires_time - now_time) - 600 <= 0:  # 만료시간의 600초 전에는 갱신. (여유시간)
            return False

        return True
