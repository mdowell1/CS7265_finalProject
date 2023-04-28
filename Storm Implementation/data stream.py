from kafka import KafkaProducer
import csv

# creates a Kafka Producer to send data to Kafka server
producer = KafkaProducer(bootstrap_servers='localhost:9092')
# sets the topic name for the Kafka topic that will hold the data
topic_name = 'airline_data'
# variable to hold an end of file message that will be send when there are no more rows
eof = "__END_OF_FILE__"

# opens the file, joins the columns with a comma, and sends the data to the topic name on the Kafka server
with open('/Users/matt/Downloads/2004.csv', 'r', errors='replace') as csvfile:
    csvreader = csv.reader(csvfile)
    next(csvreader)  # skip header
    for row in csvreader:  # loops through the rows of the CSV file
        producer.send(topic_name, ','.join(row).encode('utf-8'))

    # sends the end of file string as the last message when there are no more rows in the CSV file
    producer.send(topic_name, value=eof.encode('utf-8'))

producer.close()
