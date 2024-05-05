from kafka import KafkaConsumer
import csv
from datetime import datetime
import os
import openai
from openai import OpenAI

os.environ["OPENAI_API_KEY"] = 'API key del usuario'
client = OpenAI()

consumer = KafkaConsumer('llamadas', bootstrap_servers=['localhost:29092'],
     api_version=(0,10))

if os.path.isfile('messages.csv'):
	print("llamadas.csv ya existe...\nborrando...")
	os.remove('llamadas.csv')
	
with open('llamadas.csv', 'a') as f:
	fWriter = csv.writer(f)
	fWriter.writerow(['timestamp','llamada'])
	print("CSV iniciado")
	for llamada in consumer:
		audio_file= open(str(llamada.value.decode()), "rb")
		transcription = client.audio.transcriptions.create(
        model="whisper-1", 
        file=audio_file
        )
		print(transcription.text)
		ts = datetime.fromtimestamp(llamada.timestamp/1000).strftime("%A, %B %d, %Y %I:%M:%S")
		print("Escribiendo %s..." % transcription.text)
		fWriter.writerow([ts, transcription.text])
	
