import json
import logging

from confluent_kafka import Producer, Consumer, KafkaError, KafkaException


class KafkaProcessor(object):
    def __init__(self, kafka_conf):
        self.topic = kafka_conf['group.id']
        self.consumer_conf = {
            'auto.offset.reset': 'earliest',
            'enable.partition.eof': True,
        }
        self.consumer_conf.update(kafka_conf)
        self.producer_conf = kafka_conf
        self.producer = None
        self.consumer = None

    def init_producer(self):
        self.producer = Producer(self.producer_conf)

    def init_consumer(self):
        self.consumer = Consumer(self.consumer_conf)
        self.consumer.subscribe(self.topic.split(','))

    @staticmethod
    def delivery_report(err, msg):
        """ Called once for each message produced to indicate delivery result.
            Triggered by poll() or flush(). """
        if err is not None:
            logging.error('Message delivery failed: {}'.format(err))
        else:
            logging.info('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

    def produce(self, messages):
        if not self.producer:
            self.init_producer()
        for message in messages:
            self.producer.produce(self.topic, json.dumps(message).encode('utf-8'),
                                  callback=self.delivery_report)
            self.producer.poll(0.5)

    def consume(self):
        if not self.consumer:
            self.init_consumer()
        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        logging.info('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                        break
                    elif msg.error():
                        raise KafkaException(msg.error())
                else:
                    logging.info(msg.value().decode('utf-8'))
        finally:
            # Close down consumer to commit final offsets.
            self.consumer.close()
