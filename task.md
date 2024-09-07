Here's how you can set up the entire pipeline for a real-time streaming data solution in Python and Databricks:

### 1. Dummy Data Generator in Python (Simulates the Realtime Stream)

```python
import time
import random
import json
from azure.eventhub import EventHubProducerClient, EventData

# Connection string and Event Hub name
CONNECTION_STR = 'your_event_hub_namespace_connection_string'
EVENT_HUB_NAME = 'your_event_hub_name'

def send_event():
    # Create a producer client
    producer = EventHubProducerClient.from_connection_string(conn_str=CONNECTION_STR, eventhub_name=EVENT_HUB_NAME)
    while True:
        # Generate a random number between 50 and 100
        value = random.randint(50, 100)
        event_data = json.dumps({'value': value}).encode('utf-8')

        # Send event to Event Hub
        event_batch = producer.create_batch()
        event_batch.add(EventData(event_data))
        producer.send_batch(event_batch)

        print(f"Sent event: {value}")
        
        # Sleep for 2 seconds
        time.sleep(2)

# Start sending events
send_event()
```

This script will send a random number between 50 and 100 to the Event Hub every 2 seconds.

### 2. Create an Event Hub

- Log into Azure portal.
- Navigate to **Event Hubs**.
- Create a **Namespace** and an **Event Hub**.
- Make sure to copy the connection string and Event Hub name for use in the Python script.

### 3. Capture the Incoming Stream in Databricks Using Structured Streaming

In your Databricks notebook:

```python
from pyspark.sql.types import StructType, IntegerType
from pyspark.sql.functions import col, when

# Define the schema of the incoming data
schema = StructType().add("value", IntegerType())

# Read from Event Hub (input stream)
event_hub_connection_str = "your_event_hub_connection_string"
input_stream = (
  spark.readStream
    .format("eventhubs")
    .option("eventhubs.connectionString", event_hub_connection_str)
    .load()
)

# Parse the body and convert the bytes into proper JSON
parsed_stream = input_stream.selectExpr("CAST(body as STRING) as json").selectExpr("CAST(json as INT) as value")

# Add the 'Risk' column based on the value
risk_stream = parsed_stream.withColumn('Risk', when(col('value') > 80, 'High').otherwise('Low'))

# Write the output stream to a new Event Hub
event_hub_output_connection_str = "your_output_event_hub_connection_string"
risk_stream.writeStream \
    .format("eventhubs") \
    .option("eventhubs.connectionString", event_hub_output_connection_str) \
    .outputMode("append") \
    .start()
```

### 4. Adding the Risk Column

As shown in the Databricks code above, we use `withColumn` and `when()` functions to add a column `Risk` where:

- If the value is greater than 80, the risk is set to 'High'.
- Otherwise, the risk is set to 'Low'.

### 5. Capture the Output Stream to a Separate Event Hub

The stream from Databricks is output to another Event Hub, which will act as a source for Power BI. This is already handled in the Databricks code with the `writeStream` method.

### 6. Connect the Output Stream to Power BI Using Stream Analytics

- Go to the **Azure Stream Analytics** portal.
- Create a new **Stream Analytics job**.
- Set the input to the **output Event Hub** where Databricks is sending the processed data.
- Configure the **output** to be **Power BI**.
- Create a query that selects the number of `High` and `Low` risk values.

Example query for Stream Analytics:

```sql
SELECT 
    Risk, 
    COUNT(*) AS Count 
INTO 
    [PowerBIOutput]
FROM 
    [YourEventHubInput] 
GROUP BY 
    Risk, 
    TumblingWindow(second, 5)
```

### 7. Build a Realtime Dashboard in Power BI

- Go to Power BI and connect to the Stream Analytics output.
- Create a new dashboard and set up real-time visualizations.
- Add a **Card** or **Bar Chart** to display the count of `High` and `Low` risk values in real time.
