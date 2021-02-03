import json
import os
from datetime import datetime

from rhipe_crawler_src import envlist
from Common.tools import get_month_start_and_last_date
from rhipe_crawler_src.invoice_update_module import get_invoice_list, invoice_detail_by_invoiceid, \
    update_db_preprocess_billing_table, select_db_proprecess_day, insert_db_invoice_details
from rhipe_crawler_src.s3_module import upload_to_s3
from Common.db_connection import DBConnect

print('[INFO] CM Invoice Crawling Manager.')
search_date_str = input("Input Search Date (format: \"%Y-%m\") or typing None: ")
print(search_date_str)
CRAWLER_ENV = os.getenv('CRAWLER_ENV') or 'dev'
print(f'RUN ENV : {CRAWLER_ENV}')
search_date_datetime = None
if search_date_str != 'None':
    search_date_datetime = datetime.strptime(search_date_str, "%Y-%m")
invoice_list = get_invoice_list(search_date_datetime)

#######################################################################################
# Invoice List 출력, 선택
index = len(invoice_list) - 1
for invoice in invoice_list[::-1]:
    print('=' * 100)
    print(json.dumps(invoice, indent=4))
    print('-' * 100)
    print('INDEX :: %d' % index)
    print('=' * 100)
    index -= 1

invoice_index = int(input("Select Invoice index :: "))

# 확인
selected_invoice = invoice_list[invoice_index]
print(json.dumps(selected_invoice, indent=4))
invoice_id = selected_invoice['InvoiceId']
print("Select this invoice.. %s" % invoice_id)
input_continue = input("Continue? (y,n) : ")
if input_continue.lower() != 'y':
    print('[INFO] Cancel...')
    exit(0)

#######################################################################################
# Date 파싱

invoice_datetime = datetime.strptime("%d-%d" % (selected_invoice['UsageYear'], selected_invoice['UsageMonth']), "%Y-%m")
# invoice detail 출력

details = invoice_detail_by_invoiceid(invoice_id)

print(details)

#######################################################################################
db = DBConnect.get_instance()
# db 저장 check
input_continue = input("Insert to table? (y,n) : ")
if input_continue.lower() == 'y':
    insert_db_invoice_details(db, details)
    db.commit()
    print('[INFO] Invoice Inserting . COMPLETE')

#######################################################################################
# preprocess Table update??
input_continue = input("Update to preprocess table? (y,n) : ")
if input_continue.lower() == 'y':
    update_db_preprocess_billing_table(db, invoice_datetime, details)
    db.commit()
    print('[INFO] Invoice Update to preprocess . COMPLETE')

#######################################################################################
# swagger api to s3 upload
if CRAWLER_ENV == 'prod':
    input_continue = input("Update to s3 - api server format? (y,n) : ")
else:
    input_continue = input("Update to s3 - api server format? \n [WARNING] THIS IS NOT PROD ENV(y,n) : ")
if input_continue.lower() == 'y':
    upload_to_s3(data=select_db_proprecess_day(db=db, param_date=invoice_datetime),
                 param_date=get_month_start_and_last_date(invoice_datetime)[1],
                 is_upload=CRAWLER_ENV == 'prod')
    print('[INFO] Invoice Update to S3 . COMPLETE')

print('[Done]')