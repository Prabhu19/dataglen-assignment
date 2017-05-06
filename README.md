# dataglen-assignment

Write a Spark Structured Streaming app to extract JSON messages from kafka broker and aggregate them over a window of 2 mins.

JSON messages are being sent to five different keys- “Key1" to “Key5"
message format: key: Key1 val: {"TIMESTAMP": "2017-02-25T04:44:18", "val": 0, "key": "Key1”} 
frequency of message: 30 seconds
for each key, aggregate the values coming over 2 min window period and compute sum and mean of values received in those 2 mins

You can verify if your aggregation is correct at the aggregation topic “test_aggregated”
Aggregation message format: key: Key3 val: {"count": 4, "TIMESTAMP": "2017-02-25T04:42:00", "sum": 9, "ts": ["2017-02-25T04:42:48", "2017-02-25T04:42:20", "2017-02-25T04:43:24", "2017-02-25T04:43:48"], "key": "Key3", "vals": [4, 1, 1, 3], "mean": 2.25}
