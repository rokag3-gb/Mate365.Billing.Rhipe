import os

if os.environ['DATABASE_TYPE'] == 'mysql':
    SELECT_PREPROCESS_OF_DAY_AND_SUBSCRIPTION_SQL = """SELECT * FROM `preprocess` WHERE last_update_date = %s and subscription = %s"""
    SELECT_PREPROCESS_OF_DAY_ALL_SQL = """SELECT * FROM `preprocess` WHERE last_update_date = %s"""
    DELETE_PREPROCESS_OF_DAY_SQL = """DELETE FROM `preprocess` WHERE last_update_date = %s and subscription = %s"""

    INSERT_SUMMARY_SQL = "INSERT INTO `summary` (`tenant`, `subscription`, `body`, `last_update_date`) VALUES (%s, %s, %s, %s);"
    INSERT_DETAIL_SQL = "INSERT INTO `detail` (`tenant`, `subscription`, `body`, `last_update_date`) VALUES (%s, %s, %s, %s);"
    INSERT_PREPROCESS_SQL = "INSERT INTO `preprocess` (`tenant`, `subscription`, `body`, `last_update_date`) VALUES (%s, %s, %s, %s);"
    UPDATE_PREPROCESS_SQL = """UPDATE `preprocess` set body = JSON_SET(body, '$.Services', CAST(%s as json)) WHERE `tenant` = %s and `subscription` = %s and `last_update_date` = %s"""
    INSERT_INVOICE_SQL = """INSERT INTO `invoice` (`SubscriptionId`, `OfferName`, `ChargeStartDate`, `ChargeEndDate`, `UnitPrice`,
         `UnitPriceRrp`, `Quantity`, `BillableRatio`, `SubTotal`, `SubTotalRrp`)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    UPDATE_INVOICE_IN_PREPROCESS_SQL = """UPDATE preprocess set body = JSON_SET(body, '$.BillingTable', CAST(%s as json)) 
        WHERE preprocess_id = (SELECT preprocess_id FROM
         (SELECT preprocess_id FROM msw.preprocess WHERE subscription = %s and last_update_date >= %s and
          last_update_date <= %s order by last_update_date desc LIMIT 1) AS s)"""
    SELECT_PRODUCT_PRICE_ALL = """SELECT * FROM msw.product_price"""
elif os.environ['DATABASE_TYPE'] == 'mssql':
    SELECT_PREPROCESS_OF_DAY_AND_SUBSCRIPTION_SQL = """SELECT * FROM [AzureRhipe_preprocess] WHERE last_update_date = %s and subscription = %s"""
    SELECT_PREPROCESS_OF_DAY_ALL_SQL = """SELECT * FROM [AzureRhipe_preprocess] WHERE last_update_date = %s"""
    DELETE_PREPROCESS_OF_DAY_SQL = """DELETE FROM [AzureRhipe_preprocess] WHERE last_update_date = %s and subscription = %s"""

    INSERT_PREPROCESS_SQL = "INSERT INTO [AzureRhipe_preprocess] ([tenant], [subscription], [body], [last_update_date]) VALUES (%s, %s, %s, %s);"
    UPDATE_PREPROCESS_SQL = """UPDATE [AzureRhipe_preprocess] set body = JSON_MODIFY(body, '$.Services', JSON_QUERY(%s)) WHERE [tenant] = %s and [subscription] = %s and [last_update_date] = %s"""
    INSERT_INVOICE_SQL = """INSERT INTO [AzureRhipe_invoice] ([SubscriptionId], [OfferName], [ChargeStartDate], [ChargeEndDate], [UnitPrice],
             [UnitPriceRrp], [Quantity], [BillableRatio], [SubTotal], [SubTotalRrp])
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    UPDATE_INVOICE_IN_PREPROCESS_SQL = """UPDATE [AzureRhipe_preprocess] set body = JSON_MODIFY(body, '$.BillingTable', JSON_QUERY(%s)) 
            WHERE [preprocess_id] = (SELECT [preprocess_id] FROM
             (SELECT [preprocess_id] FROM [AzureRhipe_preprocess] WHERE [subscription] = %s and [last_update_date] >= %s and
              [last_update_date] <= %s order by [last_update_date] desc LIMIT 1) AS s)"""
    SELECT_PRODUCT_PRICE_ALL = """SELECT * FROM [AzureRhipe_product_price]"""