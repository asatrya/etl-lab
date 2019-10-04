# Q1: How many new customers did we have by store location (city)?

# Equivalent SQL Query:
# SELECT
# 	city.city,
# 	COUNT(customer.customer_id) AS customer_count
# FROM
# 	customer
# LEFT JOIN store ON
# 	store.store_id = customer.store_id
# LEFT JOIN address ON
# 	address.address_id = store.address_id
# LEFT JOIN city ON
# 	city.city_id = address.city_id
# GROUP BY
# 	city.city_id

# Typical result:
# +------------+----------------+
# | city       | customer_count |
# +------------+----------------+
# | Lethbridge |            326 |
# | Woodridge  |            273 |
# +------------+----------------+

import pandas as pd
import sqlalchemy as db

# Function to extract table to a pandas DataFrame
def extract_table_to_pandas(tablename, db_engine):
    query = "SELECT * FROM {}".format(tablename)
    return pd.read_sql(query, db_engine)

# Connect to the database using the connection URI
# make sure to use right configuration
# "mysql+pymysql://<username>:<password>@<host>:<port>/<database>" 
connection_uri = "mysql+pymysql://root:password@localhost:3306/sakila" 
db_engine_oltp = db.create_engine(connection_uri)

######################
# EXTRACT
######################

# Extract the customer table into a pandas DataFrame
customer_df = extract_table_to_pandas("customer", db_engine_oltp)

# Extract the store table into a pandas DataFrame
store_df = extract_table_to_pandas("store", db_engine_oltp)

# Extract the address table into a pandas DataFrame
address_df = extract_table_to_pandas("address", db_engine_oltp)

# Extract the city table into a pandas DataFrame
city_df = extract_table_to_pandas("city", db_engine_oltp)

######################
# TRANSFORM
######################

# Join customer and store tables
customer_store_df = pd.merge(customer_df, 
        store_df, 
        left_on='store_id', 
        right_on='store_id', 
        how='left')
# rename column
customer_store_df.rename(columns={'address_id_y':'address_id'}, inplace=True)
# drop columns
customer_store_df = customer_store_df[['customer_id', 'store_id', 'address_id']]
# Join address table
customer_store_address_df = pd.merge(customer_store_df, 
        address_df, 
        left_on='address_id', 
        right_on='address_id', 
        how='left')
# drop columns
customer_store_address_df = customer_store_address_df[['customer_id', 'city_id']]
# group by city
customer_store_address_df = customer_store_address_df.groupby('city_id').count()
# rename column
customer_store_address_df.rename(columns={'customer_id':'customer_count'}, inplace=True)
# join with city
customer_store_address_city_df = pd.merge(customer_store_address_df,
        city_df,
        left_on='city_id',
        right_on='city_id',
        how='left')
# drop columns
customer_store_address_city_df = customer_store_address_city_df[['city', 'customer_count']]

print(customer_store_address_city_df)