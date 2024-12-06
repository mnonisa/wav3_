import redis
from app.utils import load_config
import random
import time


def main():
    config_ = load_config(file_name='redis_producer_config.toml')
    r = get_redis_client(config_)

    key_ = config_['redis']['key']
    sleep_seconds = config_['redis']['producer_sleep_seconds']
    while True:
        val_ = str(random.randint(-100, 100))
        if val_ == '0':
            val_ = '1'
        r.set(key_, val_)
        print(f'+++ Val set to {val_}')
        time.sleep(sleep_seconds)


def get_redis_client(config):
    client = redis.Redis(host=config['redis_server']['server'],
                         port=config['redis_server']['port'],
                         db=config['redis_server']['db_name'],
                         decode_responses=True)

    return client


if __name__ == '__main__':
    main()
