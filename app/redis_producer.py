import redis
from utils import load_config


def main():
    config_ = load_config(file_name='redis_producer_config.toml')
    r = redis.Redis(host=config_['redis_server']['server'],
                    port=config_['redis_server']['port'],
                    db=config_['redis_server']['db_name'],
                    decode_responses=True)
    r.set('test_key', 'test_val')
    r.set('test_key_int', 5)

    print(r.get('test_key'))
    print(r.get('test_key_int'))


if __name__ == '__main__':
    main()

