from kafka import KafkaConsumer
import redis


REDIS_URL = '172.16.28.34'
REDIS_PORT = 6379
REDIS_DB = 4
KAKFA_TOPIC = "topic"
KAFKA_GROUP_ID = "monitoring-app"
KAFKA_BOOTSTRAP_SERVER = ['172.16.28.35:9092']
KAFKA_OFFSET = 'earliest'

redisClient = redis.StrictRedis(host=REDIS_URL, port=REDIS_PORT, db=REDIS_DB)
consumer = KafkaConsumer(KAKFA_TOPIC,
                         group_id=KAFKA_GROUP_ID,
                         bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
                         auto_offset_reset=KAFKA_OFFSET)


def parse_message(message):
    pass

def put_in_redis(bot_ref, user_id, user_msg, bot_reply, is_default_path):
    pass

def check_frustrated(bot_ref, user_id):
    pass

def mock_user_msg(bot_ref, user_id, emotion):
    pass


for message in consumer:
    # parse message get user id, current user message, bot message, is default path
    bot_ref, user_id, user_msg, bot_reply, is_default_path = parse_message(message)
    #add current details to redis.
    put_in_redis(bot_ref, user_id, user_msg, bot_reply, is_default_path)
    # check frustration for current user:
    is_frustrated, emotion = check_frustrated(bot_ref, user_id)
    if is_frustrated:
        # trigger path related to emotion
        mock_user_msg(bot_ref, user_id, emotion)