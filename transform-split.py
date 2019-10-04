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

# Get the rental rate column as a string
rental_rate_str = film_df.rental_rate.astype("str")

# Split up and expand the column
rental_rate_expanded = rental_rate_str.str.split(".", expand=True)

# Assign the columns to film_df
film_df = film_df.assign(
    rental_rate_dollar=rental_rate_expanded[0],
    rental_rate_cents=rental_rate_expanded[1],
)

print(film_df)