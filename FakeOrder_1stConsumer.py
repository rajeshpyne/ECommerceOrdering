from kafka import KafkaConsumer
from kafka import KafkaProducer
from faker import Factory

topic="foodOrder"
KAFKA_BROKERS="192.168.2.68:9092"
faker = Factory.create()

producer = KafkaProducer(bootstrap_servers=KAFKA_BROKERS)
consumer = KafkaConsumer(topic,bootstrap_servers=KAFKA_BROKERS,auto_offset_reset='earliest')
for msg in consumer:
        message=msg.value.decode("utf-8")
        delPerson= message+","+faker.numerify("###")
        print(delPerson)
        producer.send("delPersonAssigned", delPerson.encode("utf-8"))




