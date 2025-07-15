import adbc_driver_postgresql.dbapi as adbc
from datetime import datetime,timedelta
from time import sleep,time
from queueMap import QueueMap
def setup():
    DB_URI = "postgresql://postgres:123456789@localhost:5432/arrow_kafka"
    conn=adbc.connect(uri=DB_URI)
    return conn

def queryDB(conn,queue_map:QueueMap):
    timeToBegin = '2025-03-27 09:00:00'
    timeQuery = datetime.strptime(timeToBegin, '%Y-%m-%d %H:%M:%S')
    cursor = conn.cursor()
    topics=['TSLA','AMZN','HPE']
    while True:
        for topic in topics:
            query = "SELECT * FROM stocks2 WHERE timestamp = $1 AND stock_symbol = $2;"
            cursor.execute(query, (timeQuery,topic))
            event = cursor.fetch_arrow_table()  # ADBC supports Arrow format
            topic_metadata={"topic".encode():topic.encode()}
            topic_metadata["timestamp".encode()]=str(int((time()))).encode()
            event=event.replace_schema_metadata(topic_metadata)
            success=queue_map.putEvent(topic,event)
            timeQuery += timedelta(seconds=1)
        sleep(1)


def streamSimulator(queue_map:QueueMap):
    conn=setup()
    queryDB(conn,queue_map)