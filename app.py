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
WINDOW_SIZE = 5
DEFAULT_PATH = -0.4
SAME_RESPONSE = -0.4


class Sentiment(Enum):
    HAPPY = 0.5
    NEUTRAL = 0
    ANGRY = -0.5


class Feedback(JsonObject):
    response_ref = IntegerProperty()


class EMFObject(JsonObject):
    request_type = StringProperty(required=True)
    bot_ref = IntegerProperty(required=True)
    user_id = StringProperty(required=True)
    over_all_sentiment = FloatProperty()
    feedback = ObjectProperty(Feedback)

    @StringProperty()
    def user_message(self):
        if self.request_type == 'FEEDBACK':
            return self.event_data
        else:
            return None

    @BooleanProperty()
    def default_path(self):
        if self.request_type == 'FEEDBACK':
            if self.answer_score and self.answer_score >= 0.4:
                return False
            else:
                return True
        else:
            return False

    @IntegerProperty()
    def bot_response(self):
        if self.request_type == 'FEEDBACK':
            return self.feedback.response_ref
        else:
            return None

    @FloatProperty
    def user_msg_sentiment(self):
        if self.request_type == 'FEEDBACK':
            user_msg_list = self.event_data.split()
            if "useless" in user_msg_list:
                return Sentiment.ANGRY.value
            elif "great" in user_msg_list:
                return Sentiment.HAPPY.value
            else:
                return Sentiment.NEUTRAL.value
        else:
            return None


def parse_binary_message(value):
    json_dict = json.loads(value.decode('utf-8'))
    emf = EMFObject(json_dict)
    return emf


redisClient = redis.StrictRedis(host=REDIS_URL, port=REDIS_PORT, db=REDIS_DB)
consumer = KafkaConsumer(KAKFA_TOPIC,
                         group_id=KAFKA_GROUP_ID,
                         value_deserializer=parse_binary_message,
                         bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
                         auto_offset_reset=KAFKA_OFFSET)

consumer.poll()
consumer.seek_to_end()


def redis_key_for_user(bot_ref, user_id):
    return '%s__%s__%s' % (REDIS_KEY_PREFIX, bot_ref, user_id)


def put_in_redis(emf):
    try:
        # redisClient.rpush(redis_key_for_user(emf), json.dumps(emf.to_json()))

        key = redis_key_for_user(emf.bot_ref, emf.user_id)

        pipe = redisClient.pipeline(transaction=True)
        pipe.rpush(key, json.dumps(emf.to_json()))
        pipe.expire(key, 86400)  # 1 hour expiry
        random_int = random.randrange(1, 5)
        if random_int == 1:  # trim my list with 1/5 chance
            pipe.ltrim(key, -WINDOW_SIZE, -1)  # trims the list to store only last <WindowSize> elements in list
        results = pipe.execute()
    except Exception as e:
        LOG.error(e, exc_info=True)


def fetch_user_feedback(window_size, bot_ref, user_id):
    try:
        key = redis_key_for_user(bot_ref, user_id)
        list_of_feedback = redisClient.lrange(key, -window_size, -1)
        return list_of_feedback
    except Exception as e:
        LOG.error(e, exc_info=True)


def check_overall_emotions(current_feedback):
    """
    Fetch past emotion levels and recompute the new emotion level
    :param current_feedback:
    :return:
    """
    list_of_feedback = fetch_user_feedback(WINDOW_SIZE, current_feedback.bot_ref, current_feedback.user_id)
    overall_emotion = 0
    previous_response = None
    previous_response_default = False
    previous_response_same = False
    for feedback in list_of_feedback:
        feedback = parse_binary_message(feedback)
        overall_emotion = overall_emotion + feedback.user_msg_sentiment
        if feedback.default_path:
            if previous_response_default:
                penalty = DEFAULT_PATH * 2 #Penalty is doubled if this is a repeat
            else:
                penalty = DEFAULT_PATH
            overall_emotion = overall_emotion + penalty
            previous_response_default = True
        if previous_response and previous_response == feedback.bot_response:
            if previous_response_same:
                penalty = SAME_RESPONSE * 2 #Penalty is doubled if this is a repeat
            else:
                penalty = SAME_RESPONSE
            overall_emotion = overall_emotion + penalty
            previous_response_same = True
        previous_response = feedback.bot_response

    overall_emotion = overall_emotion + current_feedback.user_msg_sentiment
    if current_feedback.default_path:
        if previous_response_default:
            penalty = DEFAULT_PATH * 2  # Penalty is doubled if this is a repeat
        else:
            penalty = DEFAULT_PATH
        overall_emotion = overall_emotion + penalty

    if previous_response and previous_response == current_feedback.bot_response:
        if previous_response_same:
            penalty = SAME_RESPONSE * 2  # Penalty is doubled if this is a repeat
        else:
            penalty = SAME_RESPONSE
        overall_emotion = overall_emotion + penalty

    overall_emotion = overall_emotion / (len(list_of_feedback) + 1)

    LOG.info('Overall Emotion : %s ' % (overall_emotion))

    if overall_emotion <= -0.8:
        return Sentiment.ANGRY, overall_emotion
    elif overall_emotion >= 0.8:
        return Sentiment.HAPPY, overall_emotion
    else:
        return Sentiment.NEUTRAL, overall_emotion


def mock_user_msg(bot_ref, user_id, emotion):
    LOG.info('bot: %s user: %s -  %s',(bot_ref, user_id, emotion))


try:
    for message in consumer:
        if message.value.request_type == "FEEDBACK" and message.value.user_id == 'c5da2f24-48b7-439b-b8da-83944675798c':
            # parse message get user id, current user message, bot message, is default path
            LOG.info(message.value)
            # check frustration for current user:
            is_frustrated, overall_emotion = check_overall_emotions(message.value)
            message.value.over_all_sentiment = overall_emotion
            if is_frustrated == Sentiment.ANGRY:
                # trigger path related to emotion
                mock_user_msg(message.value.bot_ref, message.value.user_id, is_frustrated)
            elif is_frustrated == Sentiment.HAPPY:
                mock_user_msg(message.value.bot_ref, message.value.user_id, is_frustrated)
            # # add current message emotion levels to redis.
            put_in_redis(message.value)

except KeyboardInterrupt:
    LOG.error('KeyBoard Interrupt')
    sys.exit()
except Exception as e:
    LOG.error(e, exc_info=True)
