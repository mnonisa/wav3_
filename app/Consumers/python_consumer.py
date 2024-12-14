from confluent_kafka import Consumer
import pandas as pd
from app.utils import load_config
import redis
from sqlalchemy import create_engine
import random
from clickhouse_driver import Client
import traceback
from datetime import datetime


def main():
    config_ = load_config(file_name='consumer_config.toml')

    kafka_consumer = get_kafka_consumer(config_)
    redis_client = get_redis_client(config_)
    ch_engine = get_ch_engine(config_)
    ch_client = get_ch_client(config_)

    df_data = pd.DataFrame()
    timeout = config_["kafka"]["poll_timeout"]
    calc_ = 0
    table_creation_confirmed = False
    while True:
        try:
            msg = kafka_consumer.poll(timeout=timeout)
            if msg is None:
                pass
            elif msg.error():
                print(f'+++++ Error: {msg.error()}')
            else:
                # data collections
                kafka_val = float(msg.value().decode("utf-8"))
                redis_val = redis_client.get(config_['redis']['key'])

                # some calculation
                if random.randint(-1, 1) > 0:
                    calc_ = round(kafka_val * int(redis_val), 2)
                    calc_type = 'mult'
                else:
                    calc_ = round(kafka_val / int(redis_val), 2)
                    calc_type = 'div'

                # dataset creation and load
                curr_list = [[kafka_val, redis_val, calc_type, calc_, 'python_consumer', datetime.now()]]
                curr_df = pd.DataFrame(curr_list, columns=['kafka_val', 'redis_val', 'calc_type', 'calculation',
                                                           'consumer_name', 'ts'])
                df_data = pd.concat([df_data, curr_df])
                if df_data.shape[0] >= config_['general']['load_batch_size']:
                    if not table_creation_confirmed:
                        table_creation_confirmed = confirm_or_create_table(config_, ch_engine, ch_client)
                    rows_inserted = ch_client.insert_dataframe(
                        f'insert into {config_["clickhouse"]["schema"]}.{config_["clickhouse"]["table"]} values',
                        df_data,
                        settings={'use_numpy': True}
                    )
                    print(f'+++ Inserted {rows_inserted} rows')
                    df_data = pd.DataFrame()
        except Exception:
            print(f'+++++ Error: {traceback.format_exc()}')


def get_kafka_consumer(config_):
    kafka_consumer_conf = {'bootstrap.servers': config_["kafka"]["bootstrap.servers"],
                           'group.id': config_["kafka"]["group_id"],
                           'auto.offset.reset': config_["kafka"]["auto_offset_reset"],
                           'enable.auto.commit': config_["kafka"]["enable_auto_commit"]}

    consumer = Consumer(kafka_consumer_conf)
    consumer.subscribe([config_["kafka"]["topic"]])

    return consumer


def get_redis_client(config_):
    client = redis.Redis(host=config_['redis']['server'],
                         port=config_['redis']['port'],
                         db=config_['redis']['db_name'],
                         decode_responses=True)

    return client


def get_ch_engine(config_):
    ch_conn_str = f'clickhouse://{config_["clickhouse"]["user"]}:@{config_["clickhouse"]["host"]}'
    engine = create_engine(ch_conn_str)

    return engine


def get_ch_client(config_):
    client = Client(host=config_["clickhouse"]["host"],
                    port=config_["clickhouse"]["port"],
                    user=config_["clickhouse"]["user"])

    return client


def confirm_or_create_table(config_, ch_engine, ch_client):
    tables_query = '''
        show tables
    '''
    results = pd.read_sql(tables_query, ch_engine)
    if config_["clickhouse"]["table"] in results.name.values:
        return True
    else:
        create_table_query = f'''
            create table {config_["clickhouse"]["schema"]}.{config_["clickhouse"]["table"]} (
            kafka_val Float32,
            redis_val Float32,
            calc_type String,
            calculation Float32,
            consumer_name String,
            ts DateTime
            )
            order by ts
        '''
        ch_client.execute(create_table_query)
        return True


if __name__ == '__main__':
    main()
