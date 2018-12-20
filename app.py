import json
import logging
import random
import sys
from enum import Enum

import redis
from jsonobject import *
from kafka import KafkaConsumer

LOG = logging.getLogger()
logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(asctime)s:%(msecs)d:%(message)s")

REDIS_URL = '172.16.28.34'
REDIS_PORT = 6379
REDIS_DB = 4
KAKFA_TOPIC = "emf"
KAFKA_GROUP_ID = "monitoring-app"
KAFKA_BOOTSTRAP_SERVER = ['172.16.28.35:9092']
KAFKA_OFFSET = 'earliest'
REDIS_KEY_PREFIX = "user_sentiment"


class Sentiment(Enum):
    HAPPY = 1
    NEUTRAL = 2
    ANGRY = 3


class Message(JsonObject):
    msg_content = StringProperty()
    event_timestamp = DateTimeProperty()


class EMFObject(JsonObject):
    request_type = StringProperty(required=True)
    bot_ref = IntegerProperty(required=True)
    user_id = StringProperty(required=True)
    message = ObjectProperty(Message)

    @StringProperty()
    def user_message(self):
        if self.request_type == 'EVENT' and self.event_type == 'message_received':
            return self.eventData
        else:
            return None

    @BooleanProperty()
    def default_path(self):
        # TODO need to add better markers for identifiying default message which is not dependant on user message
        if self.request_type == 'MESSAGE' and self.message.msg_content == 'Sorry, I did not understand that.':
            return True
        else:
            return False

    @StringProperty()
    def bot_response(self):
        if self.request_type == 'MESSAGE':
            return self.message.msg_content
        else:
            return None

    @IntegerProperty
    def user_msg_sentiment(self):
        if self.request_type == 'EVENT':
            if self.request_type == 'EVENT' and self.event_type == 'message_received':
                user_msg_list = self.eventData
                if "useless" in user_msg_list:
                    return Sentiment.ANGRY.value
                elif "great" in user_msg_list:
                    return Sentiment.HAPPY.value
                else:
                    return Sentiment.NEUTRAL.value
            else:
                return Sentiment.NEUTRAL.value
        else:
            return None


def parse_message(value):
    json_dict = json.loads(value.decode('utf-8'))
    emf = EMFObject(json_dict)
    return emf


redisClient = redis.StrictRedis(host=REDIS_URL, port=REDIS_PORT, db=REDIS_DB)
consumer = KafkaConsumer(KAKFA_TOPIC,
                         group_id=KAFKA_GROUP_ID,
                         value_deserializer=parse_message,
                         bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
                         auto_offset_reset=KAFKA_OFFSET)

consumer.poll()
consumer.seek_to_end()


def redis_key_for_user(emf):
    return '%s__%s__%s' % (REDIS_KEY_PREFIX, emf.bot_ref, emf.user_id)


def put_in_redis(emf, emotion):
    try:
        # redisClient.rpush(redis_key_for_user(emf), json.dumps(emf.to_json()))

        key = redis_key_for_user(emf)

        pipe = redisClient.pipeline(transaction=True)
        pipe.rpush(key, json.dumps(emf.to_json()))
        pipe.expire(key, 86400)  # 1 hour expiry
        random_int = random.randrange(1, 5)
        if random_int == 1:  # trim my list with 1/5 chance
            pipe.ltrim(key, 0, 10)  # trims the list to store only last 10 in list
        results = pipe.execute()
    except Exception as e:
        LOG.error(e, exc_info=True)


def check_frustrated(emf):
    """
    Fetch past emotion levels and recompute the new emotion level
    :param emf:
    :return: Boolean indicating if the user is frustrated or not along with the actual emotion of current level to be logged into redis
    """
    return False, Sentiment.NEUTRAL


def mock_user_msg(bot_ref, user_id, emotion):
    pass


try:
    for message in consumer:
        # parse message get user id, current user message, bot message, is default path
        LOG.info(message.value)
        # check frustration for current user:
        is_frustrated, emotion = check_frustrated(message.value)
        if is_frustrated:
            # trigger path related to emotion
            mock_user_msg(message.value.bot_ref, message.value.user_id, emotion)
        # # add current message emotion levels to redis.
        if message.value.user_id == 'c5da2f24-48b7-439b-b8da-83944675798c':
            put_in_redis(message.value, emotion)

except KeyboardInterrupt:
    LOG.error('KeyBoard Interrupt')
    sys.exit()
except Exception as e:
    LOG.error(e, exc_info=True)
