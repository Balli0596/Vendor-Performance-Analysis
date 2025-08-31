import sqlite3
import pandas as pd
import logging
from Ingestion import ingest_db
import os
os.makedirs("logs",exist_ok=True)

# Correct logging setup
logging.basicConfig(
    filename="logs/get_vendor_summary.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

def create_vendor_summary(conn):
    query = """
    WITH FreightSummary AS (
        SELECT
            VendorNumber,
            SUM(Freight) AS FreightCost
        FROM vendor_invoice
        GROUP BY VendorNumber
    ),
    PurchaseSummary AS (
        SELECT
            p.VendorNumber,
            p.VendorName,
            p.Brand,
            p.Description,
            p.PurchasePrice,
            pp.Volume,
            pp.Price AS ActualPrice,
            SUM(p.Quantity) AS TotalPurchaseQuantity,
            SUM(p.Dollars) AS TotalPurchaseDollars
        FROM purchases p
        JOIN purchase_prices pp
            ON p.Brand = pp.Brand
        GROUP BY p.VendorNumber, p.VendorName, p.Brand, p.Description, p.PurchasePrice, pp.Price, pp.Volume
    ),
    SalesSummary AS (
        SELECT
            VendorNo,
            Brand,
            SUM(SalesDollars) AS TotalSalesDollars,
            SUM(SalesPrice) AS TotalSalesPrice,
            SUM(SalesQuantity) AS TotalSalesQuantity,
            SUM(ExciseTax) AS TotalExciseTax
        FROM sales
        GROUP BY VendorNo, Brand
    )
    SELECT 
        ps.VendorName,
        ps.VendorNumber,
        ps.Brand,
        ps.Description,
        ps.ActualPrice,
        ps.PurchasePrice,
        ps.Volume,
        ps.TotalPurchaseQuantity,
        ps.TotalPurchaseDollars,
        ss.TotalSalesQuantity,
        ss.TotalSalesDollars,
        ss.TotalSalesPrice,
        ss.TotalExciseTax,
        fs.FreightCost
    FROM PurchaseSummary ps
    LEFT JOIN SalesSummary ss
        ON ps.VendorNumber = ss.VendorNo
       AND ps.Brand = ss.Brand
    LEFT JOIN FreightSummary fs
        ON ps.VendorNumber = fs.VendorNumber
    ORDER BY ps.TotalPurchaseDollars DESC
    """
    try:
        df = pd.read_sql_query(query, conn)
        logging.info("Vendor sales summary query executed successfully.")
        return df
    except Exception as e:
        logging.error(f"Error creating vendor summary: {e}")
        raise


def clean_data(df):
    try:
        df['Volume'] = df['Volume'].astype('float64')
        df.fillna(0, inplace=True)

        # Calculated metrics with safe division
        df['GrossProfit'] = df['TotalSalesDollars'] - df['TotalPurchaseDollars']
        df['ProfitMargin'] = df.apply(
            lambda x: (x['GrossProfit'] / x['TotalSalesDollars'] * 100) if x['TotalSalesDollars'] != 0 else 0,
            axis=1
        )
        df['SalesToPurchaseRatio'] = df.apply(
            lambda x: (x['TotalSalesDollars'] / x['TotalPurchaseDollars']) if x['TotalPurchaseDollars'] != 0 else 0,
            axis=1
        )

        logging.info("Data cleaned and additional metrics added successfully.")
        return df
    except Exception as e:
        logging.error(f"Error cleaning data: {e}")
        raise


if __name__ == '__main__':
    try:
        # creating database connection
        conn = sqlite3.connect('inventory.db')

        logging.info('Creating Vendor Summary.....')
        summary_df = create_vendor_summary(conn)
        logging.info("\n" + str(summary_df.head()))

        logging.info('Cleaning Data.....')
        clean_df = clean_data(summary_df)
        logging.info("\n" + str(clean_df.head()))

        logging.info('Ingesting data.....')
        ingest_db(clean_df, 'vendor_sales_summary', conn)
        logging.info('Data ingestion complete and committed to vendor_sales_summary.')

    except Exception as e:
        logging.error(f"Pipeline failed: {e}")
    logging.shutdown()
