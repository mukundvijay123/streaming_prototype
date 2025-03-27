import adbc_driver_postgresql.dbapi as adbc
import pyarrow as pa
from datetime import datetime,timedelta
from time import sleep

def setup():
    DB_URI = "postgresql://postgres:123456789@localhost:5432/fastanalytics"
    conn=adbc.connect(uri=DB_URI)
    return conn

def formatTime(timestamp):
    formatted_timestamp = timestamp.strftime('%Y-%m-%d %H:%M:%S')
    return formatted_timestamp

def queryDB(conn,queue):
    timeToBegin='2025-03-26 09:00:00'
    time=datetime.strptime(timeToBegin,'%Y-%m-%d %H:%M:%S')
    cursor=conn.cursor()
    while(True):
        query="SELECT * FROM stock_prices WHERE timestamp = $1::TIMESTAMP ;"
        cursor.execute(query,(formatTime(time),))
        event=cursor.fetch_arrow_table()
        #print(event)
        queue.put(event)
        time=time+timedelta(seconds=1)
        sleep(1)


def streamSimulator(queue):
    conn=setup()
    queryDB(conn,queue)

