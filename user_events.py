#!/opt/homebrew/bin/python3

from kafka import KafkaProducer
import random
import time
from threading import Thread

# Kafka topic and bootstrap servers
KAFKA_TOPIC = 'app_events'
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'

# Different steps of the user journey (funnel) with respective frequencies
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

# Batch size for events
BATCH_SIZE = 10

NUM_CITIES = 20

# Function to generate events for a user in batches
def generate_events(user, city, events):
    city_name = city_mapping.get(city, 'Unknown City')

    msgs = [f'{time.time()},{user},{city_name},{event}' for event in events]

    # Send events to Kafka
    for msg in msgs:
        producer.send(KAFKA_TOPIC, bytes(msg, encoding='utf8'))
        print(f'Sending event to Kafka: {msg}')

    print(f'Sent {len(events)} events for User {user}')

# Simulate the user journey
def simulate_user_journey(user):
    # Assign a fixed random city ID to the user
    city = random.randint(1, NUM_CITIES)

    while True:
        events = []

        for funnel_step in FUNNEL_STEPS:
            step = funnel_step['step']
            frequency = funnel_step['frequency']

            # Check if user drops out at this step based on the frequency
            if random.random() > frequency:
                # User drops out, generate dropout event and break the loop
                events.append(f'dropout at {step}')
                print(f'User {user} dropped out at step: {step}')
                break

            # Generate event for the selected step
            events.append(step)

            # Check if the batch size is reached, then send the events in a batch
            if len(events) == BATCH_SIZE:
                generate_events(user, city, events)
                events = []

        # Check if any remaining events in the batch
        if events:
            generate_events(user, city, events)

        # Delay before starting the next user journey
        time.sleep(1)

if __name__ == '__main__':
    num_users = int(input("Enter the number of users: "))

    # Set up the Kafka producer
    producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)

    # Create and start a thread for each user
    threads = []
    for user in range(num_users):
        t = Thread(target=simulate_user_journey, args=(user,))
        threads.append(t)
        t.start()

    try:
        # Keep the main program running until interrupted by ^C
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        # Interrupted by ^C, stop the threads and exit
        for t in threads:
            t.join()


