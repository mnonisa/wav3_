from confluent_kafka import Consumer
import pandas as pd
from utils import load_config


def main():
    config_ = load_config(file_name='producer_config.toml')
    conf = {'bootstrap.servers': f'{config_["kafka"]["bootstrap.servers"]}',
            'group.id': 'unique_id',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False}

    consumer = Consumer(conf)
    consumer.subscribe(['test-topic'])

    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            pass
        elif msg.error():
            print(f'+++++ Error: {msg.error()}')
        else:
            print('-------------------------')
            print(pd.to_datetime(int(msg.timestamp()[1]), utc=True, unit='ms'))
            try:
                print(float(msg.value().decode("utf-8")))
            except ValueError:
                print('ValueError in parser')


if __name__ == '__main__':
    main()
