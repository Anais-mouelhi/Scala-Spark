"""
TP5 - Pipeline End-to-End
Partie 1 : Producteur Kafka - Evenements e-commerce

Simule des evenements utilisateurs (clicks, ajouts panier, achats)
et les envoie dans le topic Kafka 'ecommerce-events'.

Usage :
    pip install kafka-python
    python producer.py
"""
import json
import random
import time
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

categories = ['Electronique', 'Vetements', 'Maison', 'Livres', 'Sport']
event_types = ['click', 'add_to_cart', 'purchase']

print('Demarrage du producteur. Ctrl+C pour arreter.')
try:
    while True:
        event = {
            'user_id'    : 'user_{}'.format(random.randint(1, 100)),
            'event_type' : random.choice(event_types),
            'category'   : random.choice(categories),
            'amount'     : round(random.uniform(5.0, 500.0), 2),
            'timestamp'  : int(time.time() * 1000)
        }
        producer.send('ecommerce-events', value=event)
        print(' -> {}'.format(event))
        time.sleep(0.5)
except KeyboardInterrupt:
    print('Arret du producteur.')
    producer.close()
