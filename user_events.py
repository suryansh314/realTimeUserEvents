#!/opt/homebrew/bin/python3

from kafka import KafkaProducer
import random
import time
from threading import Thread

KAFKA_TOPIC = 'app_events'
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'

FUNNEL_STEPS = [
    {'step': 'access_application', 'frequency': 0.8},
    {'step': 'click_banner', 'frequency': 0.6},
    {'step': 'view_product_list', 'frequency': 0.5},
    {'step': 'select_product', 'frequency': 0.4},
    {'step': 'add_to_cart', 'frequency': 0.3},
    {'step': 'place_order', 'frequency': 0.1}
]

city_mapping = {
    1: 'Mumbai',
    2: 'Delhi',
    3: 'Bangalore',
    4: 'Hyderabad',
    5: 'Chennai',
    6: 'Kolkata',
    7: 'Jaipur',
    8: 'Ahmedabad',
    9: 'Pune',
    10: 'Surat',
    11: 'Lucknow',
    12: 'Kanpur',
    13: 'Nagpur',
    14: 'Indore',
    15: 'Thane',
    16: 'Bhopal',
    17: 'Visakhapatnam',
    18: 'Pimpri-Chinchwad',
    19: 'Patna',
    20: 'Vadodara'
}

BATCH_SIZE = 10

NUM_CITIES = 20

def on_send_success(record_metadata):
    print(f'Message sent successfully: {record_metadata.topic} - '
          f'partition {record_metadata.partition} - offset {record_metadata.offset}')

def on_send_error(excp):
    print(f'Error while sending message: {excp}')

def generate_events(user, city, events):
    city_name = city_mapping.get(city, 'Unknown City')

    msgs = [f'{time.time()},{user},{city_name},{event}' for event in events]

    for msg in msgs:
        try:
            producer.send(KAFKA_TOPIC, bytes(msg, encoding='utf8')).add_callback(on_send_success).add_errback(on_send_error)
            print(f'Sent {len(events)} events for User {user}')
        except Exception as e:
            print(f'Error sending event to Kafka: {e}')

def simulate_user_journey(user):
    city = random.randint(1, NUM_CITIES)
    dropped_out = False  # Flag to track dropout

    while not dropped_out:
        events = []

        for funnel_step in FUNNEL_STEPS:
            step = funnel_step['step']
            frequency = funnel_step['frequency']

            if random.random() > 0.9:
                events.append(f'dropout at {step}')
                print(f'User {user} dropped out at step: {step}')
                dropped_out = True
                break

            events.append(step)

            # if the batch size is reached then send the events in a batch
            if len(events) == BATCH_SIZE:
                generate_events(user, city, events)
                events = []

        # if any remaining events in the batch
        if events:
            generate_events(user, city, events)

        # Delay before starting the next user journey
        time.sleep(5)

if __name__ == '__main__':
    num_users = int(input("Enter the number of users: "))

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        acks='all',
        retries=3,
        retry_backoff_ms=100
    )

    threads = []
    for user in range(num_users):
        t = Thread(target=simulate_user_journey, args=(user,))
        threads.append(t)
        t.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        # Interrupted by ^C, stop the threads and exit
        for t in threads:
            t.join()
