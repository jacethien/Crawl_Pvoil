import psycopg2
import pandas as pd


# conn_string = "postgresql://postgres:ecotruck@localhost/postgres"


def connect_postgresql():
    # conn_string = "postgresql://postgres:ecotruck@docker-postgres-1/new_db"
    # conn = psycopg2.connect(conn_string)

    conn = psycopg2 .connect(
        host='postgres_container',
        user='airflow',
        password='airflow',
        database='airflow',
        port='5432'

    )
 
    # a= print("connect_Postgresql_sucessful")
    return conn


def insert_data_into_table(conn):
    mycursor = conn.cursor()
    del_table = f""" DROP TABLE IF EXISTS Pvoil
    """
    mycursor.execute(del_table)

    table = f"""
            create table if not exists  Pvoil(
            name varchar(255),
            price_oil float,
            price_gap float,
            thoi_gian date
            )
        """
    mycursor.execute(table)
    conn.commit()

    # df = pd.read_csv(r"C:\docker\dags\output.csv")
    df = pd.read_csv("/opt/airflow/dags/output.csv")
    ds = df.values.tolist()

    sql = f"""
        Insert into pvoil  (name , price_oil, price_gap, thoi_gian) VALUES (%s, %s, %s, %s)
    """
    val = ds
    mycursor.executemany(sql, val)
    conn.commit()
    mycursor.close()

def ket_qua():
    conn = connect_postgresql()
    insert_data_into_table(conn)

ket_qua()
