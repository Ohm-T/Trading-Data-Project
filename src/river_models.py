from kafka import KafkaConsumer, TopicPartition
from river import evaluate, datasets
from river import linear_model
from river import metrics
from river import optim
from river import preprocessing
from river import compose
from river import time_series
from datetime import datetime
import calendar
import json
import math

def get_month_distances(x):
    return {
        calendar.month_name[month]: math.exp(-(x['month'].month - month) ** 2)
        for month in range(1, 13)
    }


def get_ordinal_date(x):
    return {'ordinal_date': x['month'].toordinal()}


def read_and_learn(queue, topic_name):
    consumer = KafkaConsumer(topic_name, bootstrap_servers="localhost:9092", group_id='group-1', value_deserializer=lambda m: json.loads(m.decode()))
    # extract_features = compose.TransformerUnion(
    #     get_ordinal_date,
    #     get_month_distances
    # )

    model = (
        time_series.SNARIMAX(
                            p=10,
                            d=1,
                            q=10,
                            m=10,
                            sd = 1,
                            # sp=3,
                            # sq=6,
                            )
            )

    for i, msg in enumerate(consumer):
        if i==419:
            break
        else:
            msg_val = msg.value
            time = datetime.strptime(msg_val['Date'], "%Y-%m-%d %H:%M:%S")
            close = msg_val['Close']
            model = model.learn_one(close)

            # get last position to break reading
            tp = TopicPartition(topic_name,0)
            consumer.seek_to_end(tp)
            lastOffset = consumer.position(tp)
            consumer.seek_to_beginning(tp) 
            print('Learning msg_{}'.format(i))

    queue.put(model)

def predict(model, horizon = 100):
    forecast = model.forecast(horizon=horizon)
    prediction = []
    for i, y_pred in enumerate(forecast):
        prediction.append(y_pred)
        print(f'Prediction for the next {i} days :', y_pred)
    return prediction

# extract_features = compose.TransformerUnion(
#         get_ordinal_date,
#         get_month_distances
#     )
# for x, y in datasets.AirlinePassengers():
#     time = "2022-08-01 00:00:00"
#     time = datetime.strptime(time, "%Y-%m-%d %H:%M:%S")
#     extract_features = extract_features.learn_one(x)
#     print(extract_features.transform_one(x))
#     print(y)