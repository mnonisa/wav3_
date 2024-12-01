from confluent_kafka import Producer
import random
import time
from utils import load_config


def main():
    config_ = load_config(file_name='kafka_producer_config.toml')
    producer = Producer(config_['kafka_broker'])

    sleep_seconds = 1
    while True:
        val_ = str(random.randint(-100, 100))
        producer.produce(config_['kafka']['topic'], key=config_['kafka']['key'], value=val_)
        time.sleep(sleep_seconds)
        sleep_seconds = round(sleep_seconds * 1.01, 2)
        print(sleep_seconds)


if __name__ == '__main__':
    main()
