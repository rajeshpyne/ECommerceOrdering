import pandas
from faker import Factory
import random
from datetime import datetime
import time
import subprocess
import sys

faker = Factory.create()
status = 'created,delivered,returned'.split(',')

def date_between(d1, d2):
    f = '%b%d-%Y'
    return faker.date_time_between_dates(datetime.strptime(d1, f), datetime.strptime(d2, f))

def fakerecord():
    return {'awb': faker.numerify('######'),  # random number eg:235533
            'destination_city': faker.city(),  # random cities
            'product': 'random_product',  # different products
            'product_category': 'random_category',  # different categories
            'origin_city': faker.city(),  # random metro cities
            'logistics_provider_id': faker.numerify('##'),  # id's eg:1,20,28,27
            'dispatch_date': date_between('mar01-2015', 'mar15-2015'),  # datetime between mar01-2015 to mar15-2015
            'final_delivery_status': random.choice(status),  # created,delivered,returned
            'actual_delivery_date': date_between('mar16-2015', 'mar30-2015'),  # datetime between mar16-2015 to mar30-2015
            'promised_delivery_date': date_between('mar25-2015', 'apr06-2015'),  # datetime between mar25-2015 to Apr6-2015
            }

def orderGeneration():
    return {'order_id': faker.numerify('###-###'),  # random number eg:235-533
            'source_zip': faker.numerify('#####'),  # random source zip
            'destination_zip': faker.numerify('#####'), # random destination zip
            'order_time' : date_between('aug9-2018','aug10-2018'), #random order time
            'order_value' : faker.numerify('###') # random order value
            }

if __name__=='__main__':
	num=int(sys.argv[1])
	status=1
	while(status==1):
		example_dummy_data = pandas.DataFrame([orderGeneration() for _ in range(num)])
		example_dummy_data.to_csv("/home/NOODLEAI/rajesh.pyne/foodDelivery/FakeOrderGeneration.csv",mode='a',index=False,header=None)
		subprocess.call("tail -n "+str(num)+" /home/NOODLEAI/rajesh.pyne/foodDelivery/FakeOrderGeneration.csv | /opt/kafka_2.11-0.11.0.2/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic order_generated",shell=True)
		#time.sleep(1)