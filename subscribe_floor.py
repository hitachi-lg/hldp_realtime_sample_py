#!/usr/bin/python3
import sys
import uuid
from confluent_kafka import Consumer, KafkaException, KafkaError
from argparse import ArgumentParser


def basic_consume_loop(consumer, topics):
    try:
        consumer.subscribe(topics)
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None: continue

            if msg.error():
                if msg.error().code() == KafkaError.PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                # TODO: implement your processing logic HERE!
                print(msg.topic(), msg.value())
    except KeyboardInterrupt:
        print("Keyboard Interrupt")
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()


def start_kafka_consuming(floor_id, kafka_host, kafka_port=9093):
    conf = {'bootstrap.servers': "{}:{}".format(kafka_host, kafka_port),
            'group.id': str(uuid.uuid4()),
            'auto.offset.reset': 'latest', 'security.protocol': 'ssl', 'ssl.ca.location': './kafka_client.pem'}
    basic_consume_loop(consumer=Consumer(conf), topics=[floor_id])


def parse_args():
    parser = ArgumentParser(description='Simple HL-DP Kafka Consumer example')
    parser.add_argument('-s', '--server', required=True, help='Kafka Broker Host',
                        dest='kafka_host')
    parser.add_argument('-p', '--port', required=False, help='Kafka Port number', default=9093,
                        dest='kafka_port')
    parser.add_argument('-f', '--floor', required=True, help='Floor ID to subscribe tracking data',
                        dest='floor_id')
    args = parser.parse_args()
    return args


def main():
    params = parse_args()
    start_kafka_consuming(floor_id=params.floor_id,
                          kafka_host=params.kafka_host,
                          kafka_port=params.kafka_port)


if __name__ == "__main__":
    main()
