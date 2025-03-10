import psycopg2
import pandas as pd
from sqlalchemy import create_engine

df = pd.read_csv("output.csv")
# ds= df.values.tolist()
conn_string = "postgresql://postgres:ecotruck@localhost/postgres"
# db = create_engine(conn_string)
# conn = db.connect()
# def create_table(conn):

    # cursor= conn.cursor()
    
    # table= f"""
        # create table if not exists  Pvoil(
        # name varchar(255),
        # price_oil float,
        # price_gap float,
        # thoi_gian date
        # )   
    # """
    # cursor.execute(table)
    # conn.commit()


import psycopg2
conn = psycopg2.connect(conn_string)
print("Database Connected....")
cur = conn.cursor()
cur.execute("CREATE TABLE test(id serial PRIMARY KEY, sname CHAR(50), roll_num integer);")
print("Table Created....")
conn.commit()
conn.close()

# df.to_sql("pvoil", con=conn, if_exists="replace", index=False)
# conn = psycopg2.connect(
#     conn_string
# )
# # cursor= conn.cursor()

# create_table(conn)