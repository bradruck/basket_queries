# queries module
# Module holds the class => MaidHHIDMatch - manages Hive query template
# Class responsible to populate the query with api sourced variables
#

# imported dates need to be string in the form of 2018-08-26


class SarasQueries(object):

    @staticmethod
    def query1(start_date, end_date):
        start_datetime = start_date + ' 00:00:00.000'
        end_datetime = end_date + ' 23:59:59.997'

        query1 = """
        set hive.execution.engine = tez;
        set fs.s3n.block.size = 128000000;
        set fs.s3a.block.size = 128000000;
        
        INSERT OVERWRITE TABLE sndo_dataops.DLAKE_WEEKLY_TXN_TMP
        SELECT cast(h.Household as bigint) hhid,
            b.Transactiondate txn_dt,
            P.Code upc_code,
            cast(SUM(b.NetSales)as decimal(18, 2)) NetSales,
            cast(SUM(b.GrossSales)as decimal(18, 2)) GrossSales,
            SUM(b.units) Units,
            b.Store,
            b.basketID Visits,
            b.Retailer,
            concat(cast(b.BasketId as string), cast(b.Store as string), cast(b.Terminal as string), cast(
            date_format(b.TransactionDate, 'yyyyMMdd') as string), cast(b.TransactionNumber as string), cast(
            b.Retailer as string)) UniqueID
        FROM core_SpireDLX.Audience_BasketDetail b
        INNER JOIN core_SpireDLX.Card c
            ON b.card = c.CardID
        INNER JOIN core_SpireDLX.Household h
            ON c.HouseholdID = h.HouseholdID
        INNER JOIN sndo_dataops.dlake_upc_products p
            ON b.Product = p.ProductID
        WHERE b.Transactiondate between ('{start_date}') and ('{end_date}')
            and b.retailer in (4, 5, 6, 7, 8, 13, 17, 18, 20, 21, 25) 
        GROUP BY h.Household, b.Transactiondate, p.Code, b.basketID, b.Retailer, b.Store, 
            concat(cast(b.BasketId as string), cast(b.Store as string), cast(b.Terminal as string), 
            cast(date_format(b.TransactionDate, 'yyyyMMdd') as string), cast(b.TransactionNumber as string), 
            cast(b.Retailer as string));
        """.format(start_date=start_datetime, end_date=end_datetime)
        return query1

    @staticmethod
    def query2(start_date, end_date):
        start_datetime = start_date + ' 00:00:00.000'
        end_datetime = end_date + ' 23:59:59.997'

        query2 = """
        set hive.execution.engine = tez;
        set fs.s3n.block.size = 128000000;
        set fs.s3a.block.size = 128000000;
        
        INSERT OVERWRITE TABLE sndo_dataops.DLAKE_WEEKLY_TXN_BSKT
        SELECT GrossSales as BasketSales,
            NetSales as NetBasket,
            concat(cast(b.BasketId as string), cast(b.Store as string), cast(b.Terminal as string), cast(
            date_format(b.TransactionDate, 'yyyyMMdd') as string), cast(b.TransactionNumber as string), cast(
            b.Retailer as string)) UniqueID
        FROM core_SpireDLX.Audience_Basket b
        WHERE Transactiondate between ('{start_date}') and ('{end_date}');
        """.format(start_date=start_datetime, end_date=end_datetime)
        return query2
