import json


def tenant_json(t_json: dict):
    if not t_json:
        return dict()
    tenant = {
        'TenantStatus': str(t_json['TenantStatus']),
        'CustomerId': str(t_json['CustomerId']),
        'CustomerName': str(t_json['CustomerName']),
        'Subscriptions': subscriptions_json(t_json['Subscriptions']),
    }
    return tenant


def subscriptions_json(t_json: list):
    if not t_json:
        return list()
    subscriptions = []
    for subscription in t_json:
        subscriptions.append({
            'SubscriptionId': subscription['SubscriptionId'],
            'Status': subscription['Status'],
            'ProgramReferenceLabel': subscription['ProgramReferenceLabel'],
            'ProductId': subscription['ProductId'],
            'Quantity': subscription['Quantity'],
            'FirstPurchased': subscription['FirstPurchased'],
            'DisplayTemplate': subscription['DisplayTemplate'],
            'Unit': subscription['Unit'],
            'BillingCycle': subscription['BillingCycle'],
            'BillingCycleDuration': subscription['BillingCycleDuration'],
            'ProductName': subscription['ProductName'],
            'Services': services_json(subscription['Services']) if 'Services' in subscription else None
        })
    return subscriptions


def services_json(t_json: dict):
    if not t_json:
        return dict()
    services = {
        'SubscriptionName': str(t_json['SubscriptionName']),
        'StartDate': str(t_json['StartDate']),
        'EndDate': str(t_json['EndDate']),
        'Currency': str(t_json['Currency']),
        'TotalCost': str(t_json['TotalCost']),
        'LastUpdateDate': str(t_json['LastUpdateDate']),
        # 'ResourceUsageSummaries': resource_usage_summary_json(t_json['ResourceUsageSummaries']) if 'ResourceUsageSummaries' in t_json else [],
        'ResourceUsageDetails': detail_usage_line_item_json(t_json['ResourceUsageDetails']) if 'ResourceUsageDetails' in t_json else [],
    }
    return services


def detail_json(t_json: dict):
    if not t_json:
        return dict()
    detail = {
        'tenant': str(t_json['tenant']),
        'subscription': str(t_json['subscription']),
        'body': detail_usage_line_item_json(t_json['body']),
        'last_update_date': t_json['last_update_date']
    }
    return detail


def summary_json(t_json: dict):
    if not t_json:
        return dict()
    summary = {
        'tenant': str(t_json['tenant']),
        'subscription': str(t_json['subscription']),
        'body': t_json['body'],
        'last_update_date': t_json['last_update_date']
    }
    return summary


def resource_usage_summary_json(t_json: list):
    if not t_json:
        return list()
    resources = []
    for resource in resources:
        resources.append({
            'ResourceId': resource['ResourceId'],
            'ResourceName': resource['ResourceName'],
            'ResourceCategory': resource['ResourceCategory'],
            'Cost': resource['Cost'],

        })
    return resources


def detail_usage_line_item_json(t_json: list):
    if not t_json:
        return list()
    details = []
    for detail in t_json:
        AdditionalInfo = None
        if detail['AdditionalInfo'] is not None:
            try:
                AdditionalInfo = json.load(detail['AdditionalInfo'])
            except Exception as e:
                AdditionalInfo = detail['AdditionalInfo']

        Tags = None
        if detail['Tags'] is not None:
            try:
                Tags = json.load(detail['Tags'])
            except Exception as e:
                Tags = detail['Tags']

        details.append({
            "SubscriptionId": detail['SubscriptionId'],
            "CustomerName": detail['CustomerName'],
            "VendorSubscriptionId": detail['VendorSubscriptionId'],
            "BillingMpnId": detail['BillingMpnId'],
            "AdvisorMpnId": detail['AdvisorMpnId'],
            "PrimaryDomain": detail['PrimaryDomain'],
            "BillableContractAgreementId": detail['BillableContractAgreementId'],
            "UsageDate": detail['UsageDate'],
            "Currency": detail['Currency'],
            "PartnerCost": detail['PartnerCost'],
            "Rrp": detail['Rrp'],
            "VendorFxRate": detail['VendorFxRate'],
            "MeterId": detail['MeterId'],
            "MeterName": detail['MeterName'],
            "MeterCategory": detail['MeterCategory'],
            "MeterSubCategory": detail['MeterSubCategory'],
            "MeterRegion": detail['MeterRegion'],
            "MeterType": detail['MeterType'],
            "ConsumedService": detail['ConsumedService'],
            "ResourceUri": detail['ResourceUri'],
            "ResourceLocation": detail['ResourceLocation'],
            "ResourceGroup": detail['ResourceGroup'],
            "Tags":  Tags,
            "AdditionalInfo": AdditionalInfo,
            "Quantity": detail['Quantity'],
            "Unit": detail['Unit' ],
            "ProductId": detail['ProductId'],
            "ProductName": detail['ProductName'],
            "SkuId": detail['SkuId'],
            "SkuName": detail['SkuName'],
            "SubscriptionName": detail['SubscriptionName']
        })
    return details
