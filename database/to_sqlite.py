"""code for transfering the dataframe to a database"""
import sqlite3

import pandas as pd

"""
- name
- flavors
- race
- positive_effects 
- negative_effects
- medical_uses
- Rating
- Description
"""

#list of dataset columns we want in the database
needed_columns = ['name', 'flavors', 'race', 'positive_effects', 'negative_effects', 'medical_uses', 'Rating','Description']


#importing the dataset
df = pd.read_csv('https://raw.githubusercontent.com/Med-Cabinet-BW/data-science/master/Machine-Learning/merged_df_index.csv')

df_new = pd.DataFrame()


#creating a dataframe with just the above columns
for item in needed_columns:
    df_new[item] = df[item]


#initalizing the database
sl_conn = sqlite3.connect('med_cabinet1.sqlite3')
sl_curs = sl_conn.cursor()

#putting the trimmed dataframe into the database
df_new.to_sql('strain_info', con=sl_conn)

sl_curs.close()
sl_conn.commit()

print('Conversion was successful!')