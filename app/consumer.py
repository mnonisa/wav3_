from confluent_kafka import Consumer
import pandas as pd
from utils import load_config
import clickhouse_driver
import redis
from sqlalchemy import create_engine


def main():
    config_ = load_config(file_name='consumer_config.toml')
    kafka_consumer_conf = {'bootstrap.servers': f'{config_["kafka"]["bootstrap.servers"]}',
                           'group.id': 'unique_id',
                           'auto.offset.reset': 'earliest',
                           'enable.auto.commit': False}

    consumer = Consumer(kafka_consumer_conf)
    consumer.subscribe(['test-topic'])

    r = redis.Redis(host=config_['redis_server']['server'],
                    port=config_['redis_server']['port'],
                    db=config_['redis_server']['db_name'],
                    decode_responses=True)

    print(r.get('test_key'))
    print(r.get('test_key_int'))

    ch_conn_str = f'clickhouse://{config_["clickhouse"]["user"]}:@{config_["clickhouse"]["host"]}'
    ch_engine = create_engine(ch_conn_str)

    query = f'''
        select * from system.clusters
    '''
    print(pd.read_sql(query, ch_engine))

    # msg = True
    # while True:
    #     msg = consumer.poll(timeout=1.0)
    #     if msg is None:
    #         pass
    #     elif msg.error():
    #         print(f'+++++ Error: {msg.error()}')
    #     else:
    #         print('-------------------------')
    #         print(pd.to_datetime(int(msg.timestamp()[1]), utc=True, unit='ms'))
    #         try:
    #             print(float(msg.value().decode("utf-8")))
    #         except ValueError:
    #             print('ValueError in parser')

    # print('out of while')


if __name__ == '__main__':
    main()
