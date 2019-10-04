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
db_engine = db.create_engine(connection_uri)

# Extract the customer table into a pandas DataFrame
customer_df = extract_table_to_pandas("customer", db_engine)

# Extract the store table into a pandas DataFrame
store_df = extract_table_to_pandas("store", db_engine)

# Join customer and store tables
customer_store_df = pd.merge(customer_df, store_df, left_on='store_id', right_on='store_id', how='left')

# Drop unused columns
customer_store_df = customer_store_df[['customer_id', 'first_name', 'last_name', 'store_id', 'address_id_y']]

# Print result to terminal
print(customer_store_df)


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

# # Use groupBy and mean to aggregate the column
# ratings_per_film_df = film_df.groupBy('film_id').mean('rating')

# # Join the tables using the film_id column
# film_df_with_ratings = film_df.join(
#     ratings_per_film_df,
#     film_df.film_id==ratings_per_film_df.film_id
# )

# # Show the 5 first results
# print(film_df_with_ratings.show(5))