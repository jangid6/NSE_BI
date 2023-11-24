import pandas as pd
from bsedata.bse import BSE
import time
import pyodbc
from sqlalchemy import create_engine, inspect
from sqlalchemy.exc import SQLAlchemyError
import sqlalchemy as sa
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

github_excel_url = "https://raw.githubusercontent.com/jangid6/Stock-ETL-Project/main/Equity.xlsx"
engine = 'openpyxl' 
Equity = pd.read_excel(github_excel_url, engine = engine)
Equity['Security Code'] = Equity['Security Code'].astype(str)
# Equity.head() # Get the list of stocks in Nifty50

nifty50_stock_symbols = [ "ADANIENT", "ADANIPORTS", "APOLLOHOSP", "ASIANPAINT", "AXISBANK",
    "BAJAJ-AUTO", "BAJFINANCE", "BAJAJFINSV", "BPCL", "BHARTIARTL",
    "BRITANNIA", "CIPLA", "COALINDIA", "DIVISLAB", "DRREDDY", "EICHERMOT",
    "GRASIM", "HCLTECH", "HDFCBANK", "HDFCLIFE", "HEROMOTOCO", "HINDALCO",
    "HINDUNILVR", "ICICIBANK", "ITC", "INDUSINDBK", "INFY", "JSWSTEEL",
    "KOTAKBANK", "LTIM", "LT", "M&M", "MARUTI", "NTPC", "NESTLEIND",
    "ONGC", "POWERGRID", "RELIANCE", "SBILIFE", "SBIN", "SUNPHARMA",
    "TCS", "TATACONSUM", "TATAMOTORS", "TATASTEEL", "TECHM", "TITAN",
    "UPL", "ULTRACEMCO", "WIPRO"
]
nifty50_SqDF= Equity[Equity['Security Id'].isin(nifty50_stock_symbols)].reset_index(drop=True)
nifty50_SqDF.rename(columns={'Group': 'CompanyGroup'}, inplace=True)
nifty50_SqDF.columns = nifty50_SqDF.columns.str.replace(' ', '')

b = BSE(update_codes=True)
result_dfs = []
sqcode_ListNifty50 = nifty50_SqDF['SecurityCode'].values
for sqCode in sqcode_ListNifty50:
    try:
        stock_data = b.getQuote(sqCode)
        stock_df = pd.DataFrame([stock_data])
        result_dfs.append(stock_df)
        time.sleep(0.5)
    except IndexError:
        print(f"IndexError for {sqCode}: Data not available")
        
nifty50_OverviewTable_SF = pd.concat(result_dfs, ignore_index=True)
nifty50DailyTable = pd.DataFrame()

for scripCode in nifty50_OverviewTable_SF['scripCode']:
    try:
        stock_data = b.getQuote(scripCode)
        stock_df = pd.DataFrame([stock_data])
        nifty50DailyTable = pd.concat([nifty50DailyTable, stock_df.iloc[:, :-2]], ignore_index=True)
        time.sleep(1.5)
    except IndexError:
        print(f"IndexError for {scripCode}: Data not available")
nifty50DailyTable.rename(columns={'group': 'sharegroup'}, inplace=True)
nifty50DailyTable.rename(columns={'52weekHigh': 'fiftytwoweekHigh'}, inplace=True)
nifty50DailyTable.rename(columns={'52weekLow': 'fiftytwoweekLow'}, inplace=True)
nifty50DailyTable.rename(columns={'2WeekAvgQuantity': 'twoWeekAvgQuantity'}, inplace=True)
nifty50DailyTableTest_SF = nifty50DailyTable.copy()
# Convert 'updatedOn' column to datetime and extract date
nifty50DailyTableTest_SF['updatedOn'] = pd.to_datetime(nifty50DailyTableTest_SF['updatedOn'], format='%d %b %y | %I:%M %p', errors='coerce')

# Check if there are any invalid or missing date values
if pd.isna(nifty50DailyTableTest_SF['updatedOn']).any():
    print("There are invalid or missing date values in the 'updatedOn' column.")
else:
    # Extract date from 'updatedOn' column and convert the column to datetime
    nifty50DailyTableTest_SF['updatedOn'] = pd.to_datetime(nifty50DailyTableTest_SF['updatedOn'].dt.date)

if 'totalTradedValueCr' not in nifty50DailyTableTest_SF.columns:
   # Assuming nifty50DailyTableTest_SF is your DataFrame
    nifty50DailyTableTest_SF['totalTradedValueCr'] = pd.to_numeric(nifty50DailyTableTest_SF['totalTradedValue'].str.replace(',', '').str.replace(' Cr.', '', regex=True), errors='coerce')  # Convert to numeric and handle 'Cr.'
    nifty50DailyTableTest_SF['totalTradedQuantityLakh'] = pd.to_numeric(nifty50DailyTableTest_SF['totalTradedQuantity'].str.replace(',', '').str.replace(' Lakh', '', regex=True), errors='coerce')  # Convert to numeric and handle 'Lakh'
    nifty50DailyTableTest_SF['twoWeekAvgQuantityLakh'] = pd.to_numeric(nifty50DailyTableTest_SF['twoWeekAvgQuantity'].str.replace(',', '').str.replace(' Lakh', '', regex=True), errors='coerce')  # Convert to numeric and handle 'Lakh'
    nifty50DailyTableTest_SF['marketCapFullCr'] = pd.to_numeric(nifty50DailyTableTest_SF['marketCapFull'].str.replace(',', '').str.replace(' Cr.', '', regex=True), errors='coerce')  # Convert to numeric and handle 'Cr.'
    nifty50DailyTableTest_SF['marketCapFreeFloatCr'] = pd.to_numeric(nifty50DailyTableTest_SF['marketCapFreeFloat'].str.replace(',', '').str.replace(' Cr.', '', regex=True), errors='coerce')  # Convert to numeric and handle 'Cr.'

    # Drop original columns
    nifty50DailyTableTest_SF.drop(['totalTradedValue', 'totalTradedQuantity','twoWeekAvgQuantity', 'marketCapFull', 'marketCapFreeFloat'], axis=1, inplace=True)


# nifty50DailyTableTest_SF.head(n=2)

# Azure SQL Database connection parameters
# server = '<your_server_name>.database.windows.net'
# database = '<your_database_name>'
# username = '<your_username>'
# password = '<your_password>'
# driver = '{ODBC Driver 17 for SQL Server}'

server = 'localhost'
database = 'nifty50'
username = 'sa'
password = 'jangid6'
driver = 'ODBC Driver 17 for SQL Server'

# Azure SQL Database table name


# Azure SQL Database connection string
conn_str = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}'

# Create an SQLAlchemy engine
engine = create_engine(f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver={driver}")

def create_connection(conn_str):
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    return conn, cursor

try:
    # Try to connect to the SQL Server using the engine
    connection = engine.connect()
    print("Connection successful!")
    connection.close()
    conn,  cursor = create_connection(conn_str)
    
    inspector = inspect(engine)
    nifty50_table_name = 'nifty50_dailydata'
    if not inspector.has_table(nifty50_table_name):
        nifty50_table_schema = f'''
        CREATE TABLE {nifty50_table_name} (
            companyName NVARCHAR(MAX),
            currentValue FLOAT,
            change FLOAT,
            pChange FLOAT,
            updatedOn DATE,
            securityID NVARCHAR(MAX),
            scripCode NVARCHAR(MAX),
            sharegroup NVARCHAR(MAX),
            faceValue FLOAT,
            industry NVARCHAR(MAX),
            previousClose FLOAT,
            previousOpen FLOAT,
            dayHigh FLOAT,
            dayLow FLOAT,
            fiftytwoweekHigh FLOAT,
            fiftytwoweekLow FLOAT,
            weightedAvgPrice FLOAT,
            totalTradedQuantityLakh FLOAT,
            totalTradedValueCr FLOAT,
            twoWeekAvgQuantityLakh FLOAT,
            marketCapFullCr FLOAT,
            marketCapFreeFloatCr FLOAT
        );
        '''
        # Execute the schema to create the table
        cursor.execute(nifty50_table_schema)
        conn.commit()
        conn.close()

    with engine.begin() as engineConn:
        sql_max_updatedOn = pd.read_sql_query(sa.text(f'SELECT MAX(updatedOn) FROM {nifty50_table_name}'), engineConn).iloc[0, 0]
        df_max_updatedOn = nifty50DailyTableTest_SF['updatedOn'].max()
        if (pd.isnull(sql_max_updatedOn)) and (not pd.isnull(df_max_updatedOn)):
            nifty50DailyTableTest_SF.to_sql(nifty50_table_name, engine, index=False, if_exists='append', method='multi')
            print("Daily Data didn't exist, but now inserted successfully.")
        else:
            if (df_max_updatedOn > pd.Timestamp(sql_max_updatedOn)):
                nifty50DailyTableTest_SF.to_sql(nifty50_table_name, engine, index=False, if_exists='append', method='multi')
                print("Data appended successfully.")
            else:
                print("No new data to append.")
    
    company_table_name = 'nifty50_companydata'
    if not inspector.has_table(company_table_name):
        # Define the table schema based on the 'Equity' DataFrame columns
        company_table_schema = f'''
        CREATE TABLE {company_table_name} (
            securityCode NVARCHAR(MAX),
            issuerName NVARCHAR(MAX),
            securityId NVARCHAR(MAX),
            securityName NVARCHAR(MAX),
            status NVARCHAR(MAX),
            CompanyGroup NVARCHAR(MAX),
            faceValue FLOAT,
            isinNo NVARCHAR(MAX),
            industry NVARCHAR(MAX),
            instrument NVARCHAR(MAX),
            sectorName NVARCHAR(MAX),
            industryNewName NVARCHAR(MAX),
            igroupName NVARCHAR(MAX),
            iSubgroupName NVARCHAR(MAX)
        );
        '''

        # Execute the schema to create the 'company' table
        conn , cursor = create_connection(conn_str)
        cursor.execute(company_table_schema)
        conn.commit()
        conn.close()
        nifty50_SqDF.to_sql(company_table_name, engine, index=False, if_exists='append', method='multi')
        # Commit the changes and close the connection
    else:
        print("company Table already exist, hence skipping")

except SQLAlchemyError as e:
    print(f"Error connecting to SQL Server: {e}")