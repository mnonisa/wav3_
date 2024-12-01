from confluent_kafka import Consumer
import pandas as pd
from utils import load_config
import redis
from sqlalchemy import create_engine
import random
from clickhouse_driver import Client
import traceback


def main():
    config_ = load_config(file_name='consumer_config.toml')

    kafka_consumer = get_kafka_consumer(config_)
    redis_client = get_redis_client(config_)
    ch_engine = get_ch_engine(config_)
    ch_client = get_ch_client(config_)

    df_data = pd.DataFrame()
    timeout = config_["kafka"]["initial_poll_timeout"]
    calc_ = 0
    while True:
        msg = kafka_consumer.poll(timeout=timeout)
        if msg is None:
            pass
        elif msg.error():
            print(f'+++++ Error: {msg.error()}')
        else:
            kafka_val = msg.value().decode("utf-8")
            try:
                kafka_val = float(kafka_val)
                if random.randint(-1, 1) > 0:
                    inter_val = kafka_val * int(redis_client.get('test_key_int'))
                else:
                    inter_val = kafka_val / int(redis_client.get('test_key_int'))
                calc_ = round(calc_ + inter_val, 2)

                # print(calc_)

                curr_list = [[kafka_val, inter_val, calc_]]
                curr_df = pd.DataFrame(curr_list)
                df_data = pd.concat([df_data, curr_df])
                if df_data.shape[0] >= config_['general']['load_batch_size']:
                    # rows_inserted = ch_client.insert_dataframe(
                    #     f'insert into {"default"}.{"test_table"} values', df_data
                    # )
                    rows_inserted = df_data.shape[0]
                    print(f'+++ Inserted {rows_inserted} rows')
                    df_data = pd.DataFrame()
            except ValueError:
                pass
            except Exception:
                print(f'+++++ Error: {traceback.format_exc()}')

        timeout = round(timeout * config_["kafka"]["poll_timeout_growth"], 2)

    # print('out of while')


def get_kafka_consumer(config_):
    kafka_consumer_conf = {'bootstrap.servers': config_["kafka"]["bootstrap.servers"],
                           'group.id': config_["kafka"]["group_id"],
                           'auto.offset.reset': config_["kafka"]["auto_offset_reset"],
                           'enable.auto.commit': config_["kafka"]["enable_auto_commit"]}

    consumer = Consumer(kafka_consumer_conf)
    consumer.subscribe([config_["kafka"]["topic"]])

    return consumer


def get_redis_client(config_):
    client = redis.Redis(host=config_['redis_server']['server'],
                         port=config_['redis_server']['port'],
                         db=config_['redis_server']['db_name'],
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


if __name__ == '__main__':
    main()
