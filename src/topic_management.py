from kafka.admin import KafkaAdminClient, NewTopic

def topic_management(tickers=['goog','aapl','baba','bnp.pa','ing', 'nvs']):
    admin_client = KafkaAdminClient(
        bootstrap_servers="localhost:9092", 
        client_id='market'
    )
    topic_list = list()
    topic_list_pred = list()
    for ticker in tickers:
        if ticker not in admin_client.list_topics():
            topic_list.append(ticker) 
            topic_list_pred.append(ticker+'_pred')
    
    topic_list = [NewTopic(name=ticker, num_partitions=1, replication_factor=1) for ticker in topic_list]
    topic_pred_list =  [NewTopic(name=ticker, num_partitions=1, replication_factor=1) for ticker in topic_list_pred]
    admin_client.create_topics(new_topics=topic_list, validate_only=False)
    admin_client.create_topics(new_topics=topic_pred_list, validate_only=False)
    
