from kafka import KafkaProducer
import time
import os
producer = KafkaProducer(bootstrap_servers = 'localhost:29092')


directory = 'audios'

for filename in os.listdir(directory):
    f = os.path.join(directory, filename)
    if os.path.isfile(f):
        #print(f)
        producer.send("llamadas", str(f).encode('utf-8'))
        print("Enviando llamada {}".format(f))
        print("Llamada enviada")
        time.sleep(2)