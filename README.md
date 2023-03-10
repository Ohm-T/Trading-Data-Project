# M2 DS S2 data stream : Trading-Data-Project with River, Kafka and Yfinance
 (Trading Data) Collect trading data using Yahoo finance API and use online regression (from river) to predict markets stocks of global companies.

For several countries, we used major industries stock data (US Google and Apple, France BNP Paribas, China Alibaba, Switzerland Novartis)
We tes several data streams, we used online learning methods with RIVER and compare the performances with batch regression models (LSTM and autoregressive model). We concluded on the performances on recent stock market data.

Online resources: 
Python library to collect Yahoo Finance data in streaming https://pypi.org/project/yfinance/
Time-series statistics and moving averages (MACD) for features engineering https://www.statsmodels.org/stable/tsa.html


# Installation
River:
- Documentation : https://riverml.xyz/latest/
- Installation : $ pip install river

Yfinance :
- Documentation : https://pypi.org/project/yfinance/
- Installation : $ pip install yfinance

Java jdk8 necessary for kafka

Kafka and Kafka-python: 
- $ pip install kafka-python
- $ tar -xzf kafka_2.12-3.3.1.tgz
- $ cd kafka_2.12-3.3.1

# How to run Batch learning models ?
Run the cells of LSTM__Time_series.ipynb wich contains several parts:
- Analyses and basic manipulations of yfinance with Bollinger band strategy interactive figure
- LSTM model training on yfinance datas (you can modify if you want the change the training and test sets on other companies)
- Autoregressive model, SARIMAX, graph plots

# How to run Online Learning models ?

Run zookeeper :
- Edit : $ config/zookeeper.properties
- Execute : $ bin/zookeeper-server-start.sh config/zookeeper.properties

Run kafka server :
- Edit : config/server.properties
- Execute : bin/kafka-server-start.sh config/server.properties

Run src/main.py (sometimes you have to stop and run a second time to get the predictions), the tested model here is simplest SNARIMAX model (you can modify the parameters of the model or try another model in river_models.py)

The files are the following:
- load_data.py : load data from yfinance, KNNImputer of the Nans (mostly for batch regression model)
- topic_management.py : create automatically the necessary topics
- model_batch.py : Model of for batch learning (LSTM in our case)
- start_server.py : start automatically zookeeper and kafka servers but not used for now
- ingest_data.py : load data from yfinance, KNN imputer for the nans and send them into producer
- river_models.py : models from river that learns and predict
- main.py : run all for online learning
- monitor_kafka.py : Check the flows of messages

(consumer.py : wrap process, archive_data : save everything in txt file and delete_topics: stop and delete properly topics not finished files)

