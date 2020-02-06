"""Convert CSV file into usable PostgreSQL database on Elephant SQL."""
import sqlite3
import psycopg2
import pandas as pd
from dotenv import load_dotenv
import os

# Load .env file
load_dotenv()

# Create postgres connection
pg_conn = psycopg2.connect(
    dbname=os.getenv("DBNAME"), user=os.getenv("USER"),
    password=os.getenv("PASSWORD"), host=os.getenv("HOST")
    )
pg_curs = pg_conn.cursor()


# list of dataset columns we want in the database
needed_columns = [
    'name', 'flavors', 'race', 'positive_effects', 'negative_effects',
    'medical_uses', 'Rating', 'Description'
    ]


# importing the dataset
df = pd.read_csv('../Machine-Learning/final_df_percentsep.csv', sep='%')

# ('https://raw.githubusercontent.com/Med-Cabinet-BW/data-science/master/Machine-Learning/merged_df_index.csv')

df_new = pd.DataFrame()


# creating a dataframe with just the above columns
for item in needed_columns:
    df_new[item] = df[item]

# Replace NaNs with NULL to comply with SQL
df_new = df_new.fillna('NULL')

# initalizing the database
sl_conn = sqlite3.connect('med_cabinet1.sqlite3')
sl_curs = sl_conn.cursor()

# putting the trimmed dataframe into the database
df_new.to_sql('strain_info', con=sl_conn, if_exists='replace')

# sl_curs.close()
sl_conn.commit()

print('Conversion was successful!')

# Create queried object of all data
query = """ SELECT * FROM strain_info; """
strains_sql = sl_curs.execute(query).fetchall()

# Create insert debugger
# import pdb; pdb.set_trace()

# Create postgreSQL table
create_strains_table = """
    CREATE TABLE strains (
        index SERIAL PRIMARY KEY,
        name VARCHAR(30),
        flavors VARCHAR(75),
        race VARCHAR(6),
        positive_effects VARCHAR(110),
        negative_effects VARCHAR(60),
        medical_uses VARCHAR(90),
        rating FLOAT,
        description VARCHAR(2000)
    )
    """
pg_curs.execute(create_strains_table)
pg_conn.commit()

# Insert data into table
for strain in strains_sql:
    insert_strain = """
    INSERT INTO strains
    (index, name, flavors, race, positive_effects,
    negative_effects, medical_uses, rating, description)
    VALUES """ + str(strain) + ";"
    pg_curs.execute(insert_strain)

# Commit changes
pg_conn.commit()

print('PostgreSQL table creation was successful!')
