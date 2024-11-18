from confluent_kafka import Producer
import random
import time
from utils import load_config


def main():
    config_ = load_config(file_name='kafka_producer_config.toml')
    producer = Producer(config_['kafka_broker'])

    while True:
        print(f'topics: {producer.list_topics().topics}')
        val_ = str(random.randint(0, 100)/100)
        producer.produce(config_['kafka']['topic'], key=config_['kafka']['key'], value=val_)
        print(f'pushed: {val_}')
        time.sleep(1)


if __name__ == '__main__':
    main()
