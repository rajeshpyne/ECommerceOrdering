from kafka import KafkaConsumer
from kafka import KafkaProducer
from faker import Factory
from datetime import datetime

topic="delPersonAssigned"
KAFKA_BROKERS="192.168.2.68:9092"
faker = Factory.create()

def date_between(d1, d2):
    f = '%b%d-%Y'
    return faker.date_time_between_dates(datetime.strptime(d1, f), datetime.strptime(d2, f))

producer = KafkaProducer(bootstrap_servers=KAFKA_BROKERS)
consumer = KafkaConsumer(topic,bootstrap_servers=KAFKA_BROKERS,auto_offset_reset='earliest')
for msg in consumer:
        message=msg.value.decode("utf-8")
        delTime= message+","+str(date_between('aug11-2018','aug12-2018'))
        print(delTime)
        producer.send("delTimeAssigned", delTime.encode("utf-8"))
