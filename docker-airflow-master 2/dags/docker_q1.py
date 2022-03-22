import psycopg2
from datetime import datetime


def db_connection():
    connection = psycopg2.connect(host="postgres", database="airflow", user="airflow", password="airflow",
                                  port='5432')
    #connection = psycopg2.connect(database="postgres", user="postgres", password="naresh", host="localhost", port="5432")
    cursor = connection.cursor()
    cursor.execute("select exists(select * from information_schema.tables where table_name=%s)", ('datetimeinfo',))
    res = cursor.fetchone()[0]
    print(res)

    if res:
        print("Table already exist.")
    else:
        create_table = """CREATE TABLE datetimeinfo(
                    DATETIME VARCHAR(20));"""
        cursor.execute(create_table)
        print("Table created.")

    insert = """INSERT INTO datetimeinfo(DATETIME) VALUES(%s);"""
    now = datetime.now()
    dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
    cursor.execute(insert, [dt_string])

    show = """SELECT * FROM datetimeinfo;"""
    cursor.execute(show)
    data = cursor.fetchall()
    print(data)
    connection.commit()
    connection.close()


