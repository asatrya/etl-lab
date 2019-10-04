import pandas as pd
import sqlalchemy as db

##########################################
# CONFIGS
##########################################

# Connection URI of OLTP database
connection_uri_oltp = "mysql+pymysql://root:password@localhost:3306/sakila" 
oltp_db_engine = db.create_engine(connection_uri_oltp)

# Connection URI of OLAP database
connection_uri_olap = "mysql+pymysql://root:password@localhost:3306/sakila_dw" 
olap_db_engine = db.create_engine(connection_uri_olap)

##########################################
# EXTRACT
##########################################

# Function to extract latest date from a table
def extract_latest_date(db_engine, table, field):
    query = "SELECT MAX({}) AS max_date FROM {}".format(field, table)
    result_df = pd.read_sql(query, db_engine)
    return result_df.loc[0, 'max_date']

# extract latest create_date from customer table
print("Dates extracted from OLTP:")

customer_latest_create_date = extract_latest_date(oltp_db_engine, 'customer', 'create_date')
print("Latest create date={}".format(customer_latest_create_date))

# extract latest rental_date from rental table
customer_latest_rental_date = extract_latest_date(oltp_db_engine, 'rental', 'rental_date')
print("Latest rental date={}".format(customer_latest_rental_date))

# extract latest payment_date from payment table
customer_latest_payment_date = extract_latest_date(oltp_db_engine, 'payment', 'payment_date')
print("Latest payment date={}".format(customer_latest_payment_date))

# select global latest date
dates_df = pd.DataFrame([customer_latest_create_date, customer_latest_rental_date, customer_latest_payment_date])
global_latest_date = dates_df.max().loc[0]
print("Global latest date={}".format(global_latest_date))

# extract latest value from date dimension
print("Dates extracted from OLAP:")
dateDim_latest = extract_latest_date(olap_db_engine, 'dimDate', 'date')
if dateDim_latest == None:
    dateDim_latest_next_day = pd.datetime(2005, 2, 14)
else:
    dateDim_latest_next_day = dateDim_latest + pd.Timedelta(1, unit='D')
print("DW Latest day+1day={}".format(dateDim_latest_next_day))


##########################################
# TRANSFORM
##########################################

def label_weekend(row):
    if row['dayofweek'] == 5 or row['dayofweek'] == 6:
        return 1
    else:
        return 0 

def create_date_table(start=dateDim_latest_next_day, end=global_latest_date):
    df = pd.DataFrame({"date": pd.date_range(start, end)})
    df["dayofweek"] = df.date.dt.dayofweek
    df["day"] = df.date.dt.day
    df["week"] = df.date.dt.weekofyear
    df["month"] = df.date.dt.month
    df["quarter"] = df.date.dt.quarter
    df["year"] = df.date.dt.year
    df["is_weekend"] = df.apply(lambda row: label_weekend(row), axis=1)
    df["is_holiday"] = df.apply(lambda row: label_weekend(row), axis=1)
    df["date_key"] = df.date.dt.strftime('%Y%m%d')
    return df[['date_key', 'day', 'date', 'year', 'quarter', 'month', 'week', 'is_weekend', 'is_holiday']]

if global_latest_date > dateDim_latest_next_day:
    date_range_df = create_date_table()
    print('New date dimension range created:')
    print(date_range_df)

    ##########################################
    # LOAD
    ##########################################

    date_range_df.to_sql('dimDate', olap_db_engine, if_exists='append', index=False)

else:
    print('No date range generation needed')