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
    INSERT_INVOICE_SQL = """INSERT INTO [AzureRhipe_invoice] ([invoiceid], [SubscriptionId], [OfferName], [ChargeStartDate], [ChargeEndDate], [UnitPrice],
             [UnitPriceRrp], [Quantity], [BillableRatio], [SubTotal], [SubTotalRrp])
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    UPDATE_INVOICE_IN_PREPROCESS_SQL = """UPDATE [AzureRhipe_preprocess] set body = JSON_MODIFY(body, '$.BillingTable', JSON_QUERY(%s)) 
            WHERE [preprocess_id] = (SELECT [preprocess_id] FROM
             (SELECT TOP (1) [preprocess_id] FROM [AzureRhipe_preprocess] WHERE [subscription] = %s and [last_update_date] >= %s and
              [last_update_date] <= %s order by [last_update_date] desc) AS s)"""
    SELECT_PRODUCT_PRICE_ALL = """SELECT * FROM [AzureRhipe_product_price]"""
    INSERT_CUSTOMER = """
        
    DECLARE @CustomerId varchar(50);
    DECLARE @CrmAccountId varchar(50);
    DECLARE @IsPartnerCustomer bit;
    DECLARE @CustomerName nvarchar(300);
    DECLARE @CustomerNotificationEmail varchar(100);
    DECLARE @ParentCustomerId varchar(50);
    DECLARE @RegistrationNumber varchar(50);
    DECLARE @SignedWithRhipe varchar(50);
    DECLARE @WebUrl varchar(200);
    DECLARE @MainPhone varchar(50);
    DECLARE @Fax varchar(50);
    DECLARE @Street1 nvarchar(100);
    DECLARE @Street2 nvarchar(100);
    DECLARE @Street3 nvarchar(100);
    DECLARE @City nvarchar(100);
    DECLARE @State nvarchar(100);
    DECLARE @Postcode varchar(20);
    DECLARE @Country nvarchar(100);
    DECLARE @CountryIsoCode varchar(50);
    DECLARE @CrmId varchar(50);
    DECLARE @FinanceAccountId varchar(50);
    DECLARE @FinanceAccounts varchar(50);
    DECLARE @DirectDebitWholeAccount varchar(50);
    DECLARE @Email varchar(50);
    DECLARE @BillingStreet1 nvarchar(100);
    DECLARE @BillingStreet2 nvarchar(100);
    DECLARE @BillingStreet3 nvarchar(100);
    DECLARE @BillingCity nvarchar(100);
    DECLARE @BillingState nvarchar(100);
    DECLARE @BillingPostcode varchar(20);
    DECLARE @BillingCountry nvarchar(100);
    DECLARE @BillingCountryIsoCode varchar(50);
    DECLARE @SalesTerritoryName varchar(50);
    DECLARE @SalesPersonFirstName varchar(50);
    DECLARE @SalesPersonLastName varchar(50);
    DECLARE @AccountManagerFirstName varchar(50);
    DECLARE @AccountManagerLastName varchar(50);
    DECLARE @HowDidYouHearAboutRhipe varchar(50);
    DECLARE @HowDidYouHearAboutRhipeOther varchar(50);
    DECLARE @IndustryType varchar(50);
    DECLARE @IndustryTypeOther varchar(50);
    DECLARE @TenantId varchar(50);
    DECLARE @ProgramId varchar(50);
    DECLARE @AgreementStartDate varchar(50);
    DECLARE @AgreementEndDate varchar(50);
    DECLARE @ContractAgreementId varchar(50);
    DECLARE @BillingPeriod varchar(50);
    DECLARE @ProgramReferenceId varchar(50);
    DECLARE @ProgramReferenceLabel varchar(200);
    DECLARE @ProgramName varchar(200);
    DECLARE @Customer varchar(50);
    DECLARE @IsConsumptionProgram varchar(50);
    DECLARE @Contacts varchar(50);
    DECLARE @CreditCard varchar(50);
    DECLARE @PaymentMethodDetails varchar(100);
    DECLARE @HasContractAgreement varchar(50);
    DECLARE @IsActive bit;
    DECLARE @ReferringPartnerName varchar(50);
    DECLARE @IsRhipeEndCustomer varchar(50);
    DECLARE @IsRhipePartnerCustomer varchar(50);
    DECLARE @RegDate datetime;
    
    SET @CustomerId = %s;
    SET @CrmAccountId = %s;
    SET @IsPartnerCustomer = %s;
    SET @CustomerName = %s;
    SET @CustomerNotificationEmail = %s;
    SET @ParentCustomerId = %s;
    SET @RegistrationNumber = %s;
    SET @SignedWithRhipe = %s;
    SET @WebUrl = %s;
    SET @MainPhone = %s;
    SET @Fax = %s;
    SET @Street1 = %s;
    SET @Street2 = %s;
    SET @Street3 = %s;
    SET @City = %s;
    SET @State = %s;
    SET @Postcode = %s;
    SET @Country = %s;
    SET @CountryIsoCode = %s;
    SET @CrmId = %s;
    SET @FinanceAccountId = %s;
    SET @FinanceAccounts = %s;
    SET @DirectDebitWholeAccount = %s;
    SET @Email = %s;
    SET @BillingStreet1 = %s;
    SET @BillingStreet2 = %s;
    SET @BillingStreet3 = %s;
    SET @BillingCity = %s;
    SET @BillingState = %s;
    SET @BillingPostcode = %s;
    SET @BillingCountry = %s;
    SET @BillingCountryIsoCode = %s;
    SET @SalesTerritoryName = %s;
    SET @SalesPersonFirstName = %s;
    SET @SalesPersonLastName = %s;
    SET @AccountManagerFirstName = %s;
    SET @AccountManagerLastName = %s;
    SET @HowDidYouHearAboutRhipe = %s;
    SET @HowDidYouHearAboutRhipeOther = %s;
    SET @IndustryType = %s;
    SET @IndustryTypeOther = %s;
    SET @TenantId = %s;
    SET @ProgramId = %s;
    SET @AgreementStartDate = %s;
    SET @AgreementEndDate = %s;
    SET @ContractAgreementId = %s;
    SET @BillingPeriod = %s;
    SET @ProgramReferenceId = %s;
    SET @ProgramReferenceLabel = %s;
    SET @ProgramName = %s;
    SET @Customer = %s;
    SET @IsConsumptionProgram = %s;
    SET @Contacts = %s;
    SET @CreditCard = %s;
    SET @PaymentMethodDetails = %s;
    SET @HasContractAgreement = %s;
    SET @IsActive = %s;
    SET @ReferringPartnerName = %s;
    SET @IsRhipeEndCustomer = %s;
    SET @IsRhipePartnerCustomer = %s;
    SET @RegDate = %s;
    
    IF EXISTS (SELECT 1
               FROM [dbo].[AzureRhipe_Customer]
               WHERE [tenantId] = @tenantId)
    BEGIN
        UPDATE [dbo].[AzureRhipe_Customer]
           SET [CustomerId] = @CustomerId
              ,[CrmAccountId] = @CrmAccountId
              ,[IsPartnerCustomer] = @IsPartnerCustomer
              ,[CustomerName] = @CustomerName
              ,[CustomerNotificationEmail] = @CustomerNotificationEmail
              ,[ParentCustomerId] = @ParentCustomerId
              ,[RegistrationNumber] = @RegistrationNumber
              ,[SignedWithRhipe] = @SignedWithRhipe
              ,[WebUrl] = @WebUrl
              ,[MainPhone] = @MainPhone
              ,[Fax] = @Fax
              ,[Street1] = @Street1
              ,[Street2] = @Street2
              ,[Street3] = @Street3
              ,[City] = @City
              ,[State] = @State
              ,[Postcode] = @Postcode
              ,[Country] = @Country
              ,[CountryIsoCode] = @CountryIsoCode
              ,[CrmId] = @CrmId
              ,[FinanceAccountId] = @FinanceAccountId
              ,[FinanceAccounts] = @FinanceAccounts
              ,[DirectDebitWholeAccount] = @DirectDebitWholeAccount
              ,[Email] = @Email
              ,[BillingStreet1] = @BillingStreet1
              ,[BillingStreet2] = @BillingStreet2
              ,[BillingStreet3] = @BillingStreet3
              ,[BillingCity] = @BillingCity
              ,[BillingState] = @BillingState
              ,[BillingPostcode] = @BillingPostcode
              ,[BillingCountry] = @BillingCountry
              ,[BillingCountryIsoCode] = @BillingCountryIsoCode
              ,[SalesTerritoryName] = @SalesTerritoryName
              ,[SalesPersonFirstName] = @SalesPersonFirstName
              ,[SalesPersonLastName] = @SalesPersonLastName
              ,[AccountManagerFirstName] = @AccountManagerFirstName
              ,[AccountManagerLastName] = @AccountManagerLastName
              ,[HowDidYouHearAboutRhipe] = @HowDidYouHearAboutRhipe
              ,[HowDidYouHearAboutRhipeOther] = @HowDidYouHearAboutRhipeOther
              ,[IndustryType] = @IndustryType
              ,[IndustryTypeOther] = @IndustryTypeOther
              ,[TenantId] = @TenantId
              ,[ProgramId] = @ProgramId
              ,[AgreementStartDate] = @AgreementStartDate
              ,[AgreementEndDate] = @AgreementEndDate
              ,[ContractAgreementId] = @ContractAgreementId
              ,[BillingPeriod] = @BillingPeriod
              ,[ProgramReferenceId] = @ProgramReferenceId
              ,[ProgramReferenceLabel] = @ProgramReferenceLabel
              ,[ProgramName] = @ProgramName
              ,[Customer] = @Customer
              ,[IsConsumptionProgram] = @IsConsumptionProgram
              ,[Contacts] = @Contacts
              ,[CreditCard] = @CreditCard
              ,[PaymentMethodDetails] = @PaymentMethodDetails
              ,[HasContractAgreement] = @HasContractAgreement
              ,[IsActive] = @IsActive
              ,[ReferringPartnerName] = @ReferringPartnerName
              ,[IsRhipeEndCustomer] = @IsRhipeEndCustomer
              ,[IsRhipePartnerCustomer] = @IsRhipePartnerCustomer
              ,[RegDate] = @RegDate
         WHERE [TenantId] = @TenantId

    END
    ELSE
    BEGIN    
        INSERT INTO [dbo].[AzureRhipe_Customer]
                   ([CustomerId]
                   ,[CrmAccountId]
                   ,[IsPartnerCustomer]
                   ,[CustomerName]
                   ,[CustomerNotificationEmail]
                   ,[ParentCustomerId]
                   ,[RegistrationNumber]
                   ,[SignedWithRhipe]
                   ,[WebUrl]
                   ,[MainPhone]
                   ,[Fax]
                   ,[Street1]
                   ,[Street2]
                   ,[Street3]
                   ,[City]
                   ,[State]
                   ,[Postcode]
                   ,[Country]
                   ,[CountryIsoCode]
                   ,[CrmId]
                   ,[FinanceAccountId]
                   ,[FinanceAccounts]
                   ,[DirectDebitWholeAccount]
                   ,[Email]
                   ,[BillingStreet1]
                   ,[BillingStreet2]
                   ,[BillingStreet3]
                   ,[BillingCity]
                   ,[BillingState]
                   ,[BillingPostcode]
                   ,[BillingCountry]
                   ,[BillingCountryIsoCode]
                   ,[SalesTerritoryName]
                   ,[SalesPersonFirstName]
                   ,[SalesPersonLastName]
                   ,[AccountManagerFirstName]
                   ,[AccountManagerLastName]
                   ,[HowDidYouHearAboutRhipe]
                   ,[HowDidYouHearAboutRhipeOther]
                   ,[IndustryType]
                   ,[IndustryTypeOther]
                   ,[TenantId]
                   ,[ProgramId]
                   ,[AgreementStartDate]
                   ,[AgreementEndDate]
                   ,[ContractAgreementId]
                   ,[BillingPeriod]
                   ,[ProgramReferenceId]
                   ,[ProgramReferenceLabel]
                   ,[ProgramName]
                   ,[Customer]
                   ,[IsConsumptionProgram]
                   ,[Contacts]
                   ,[CreditCard]
                   ,[PaymentMethodDetails]
                   ,[HasContractAgreement]
                   ,[IsActive]
                   ,[ReferringPartnerName]
                   ,[IsRhipeEndCustomer]
                   ,[IsRhipePartnerCustomer]
                   ,[RegDate])
             VALUES
                   (@CustomerId,
                    @CrmAccountId,
                    @IsPartnerCustomer,
                    @CustomerName,
                    @CustomerNotificationEmail,
                    @ParentCustomerId,
                    @RegistrationNumber,
                    @SignedWithRhipe,
                    @WebUrl,
                    @MainPhone,
                    @Fax,
                    @Street1,
                    @Street2,
                    @Street3,
                    @City,
                    @State,
                    @Postcode,
                    @Country,
                    @CountryIsoCode,
                    @CrmId,
                    @FinanceAccountId,
                    @FinanceAccounts,
                    @DirectDebitWholeAccount,
                    @Email,
                    @BillingStreet1,
                    @BillingStreet2,
                    @BillingStreet3,
                    @BillingCity,
                    @BillingState,
                    @BillingPostcode,
                    @BillingCountry,
                    @BillingCountryIsoCode,
                    @SalesTerritoryName,
                    @SalesPersonFirstName,
                    @SalesPersonLastName,
                    @AccountManagerFirstName,
                    @AccountManagerLastName,
                    @HowDidYouHearAboutRhipe,
                    @HowDidYouHearAboutRhipeOther,
                    @IndustryType,
                    @IndustryTypeOther,
                    @TenantId,
                    @ProgramId,
                    @AgreementStartDate,
                    @AgreementEndDate,
                    @ContractAgreementId,
                    @BillingPeriod,
                    @ProgramReferenceId,
                    @ProgramReferenceLabel,
                    @ProgramName,
                    @Customer,
                    @IsConsumptionProgram,
                    @Contacts,
                    @CreditCard,
                    @PaymentMethodDetails,
                    @HasContractAgreement,
                    @IsActive,
                    @ReferringPartnerName,
                    @IsRhipeEndCustomer,
                    @IsRhipePartnerCustomer,
                    @RegDate
                    )
    END
    """

DELETE_RHIPE_PRODUCT_PRICE = "DELETE FROM [dbo].[AzureRhipe_product_price]"
INSERT_RHIPE_PRODUCT_PRICE = """

INSERT INTO [dbo].[AzureRhipe_product_price]
           ([product_name]
           ,[product_id_SKU]
           ,[product_id_rhipe]
           ,[partner_price]
           ,[retail_price]
           ,[retail_unit_price]
           ,[datetime_stamp])
     VALUES
           (%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s)


"""
CHECK_INVOICE_LIST = """SELECT TOP(1) [InvoiceMonth]
  FROM [dbo].[AzureRhipe_invoice]
  WHERE [InvoiceMonth] = %s"""