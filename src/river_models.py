from kafka import KafkaConsumer
from river import evaluate
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
        calendar.month_name[month]: math.exp(-(x.month - month) ** 2)
        for month in range(1, 13)
    }


def get_ordinal_date(x):
    return {'ordinal_date': x.toordinal()}


def read_and_predict(topic_name, horizon = 5):
    consumer = KafkaConsumer(topic_name, bootstrap_servers="localhost:9092", group_id='group-1', value_deserializer=lambda m: json.loads(m.decode()))
    extract_features = compose.TransformerUnion(
        get_ordinal_date,
        get_month_distances
    )
    prediction = []
    model = (
        extract_features |
        time_series.SNARIMAX(
                            p=1,
                            d=0,
                            q=0,
                            m=12,
                            sp=3,
                            sq=6,
                            regressor=(
                                    linear_model.LinearRegression(
                                    intercept_init=110,
                                    optimizer=optim.SGD(0.01),
                                    intercept_lr=0.3
                                ))
                            )
            )

    for i, msg in enumerate(consumer):
        msg = msg.value
        time = datetime.strptime(msg['Date'], "%Y-%m-%d %H:%M:%S")
        close = msg['Close']
        model = model.learn_one(time, close)
        if i > 600:
            forecast = model.forecast(horizon=horizon)
            print(forecast)
            prediction.append(forecast)
            for y_pred in forecast:
                print('Prediction for the next 5 days :', y_pred)
    return prediction