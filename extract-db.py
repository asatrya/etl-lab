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

# Extract the film table into a pandas DataFrame
film_df = extract_table_to_pandas("film", db_engine)
print(film_df)

# Extract the customer table into a pandas DataFrame
customer_df = extract_table_to_pandas("customer", db_engine)
print(customer_df)