import producer_server

"""
run the following command in terminal to start the kafka consumer
/usr/bin/kafka-console-consumer --topic "crime_topic" --bootstrap-server localhost:9092

run kafka_server.py file in terminal to generate produce data 

add the following settings 
offsets.topic.replication.factor=1
to 
config/server.properties
otherwise will have error
"""

def run_kafka_server():
	# TODO get the json file path
    input_file = "police-department-calls-for-service.json"

    # TODO fill in blanks
    producer = producer_server.ProducerServer(
        input_file=input_file,
        topic="crime_topic",
        bootstrap_servers="localhost:9092",
        client_id="yoyoyo",
    )

    return producer


def feed():
    producer = run_kafka_server()
    producer.generate_data()


if __name__ == "__main__":
    feed()
