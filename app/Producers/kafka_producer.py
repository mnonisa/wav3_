from confluent_kafka import Producer
import random
import time
from app.utils import load_config


def main():
    config_ = load_config(file_name='kafka_producer_config.toml')
    producer = Producer(config_['kafka_broker'])

    sleep_seconds = config_['kafka']['producer_sleep_seconds']
    while True:
        val_ = str(random.randint(-100, 100))
        producer.produce(config_['kafka']['topic'], key=config_['kafka']['key'], value=val_)
        print(f'+++ Producer pushed {val_}')
        time.sleep(sleep_seconds)


if __name__ == '__main__':
    main()
