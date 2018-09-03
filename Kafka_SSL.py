#!/usr/bin/env python
import threading, logging, time
import multiprocessing

from kafka import KafkaConsumer, KafkaProducer


class Producer(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()

    def stop(self):
        self.stop_event.set()

    def get_Kafka_brokers_list(type):
        broker_string = ''
        ip_list=kafka_ip_address.split(',')
        for item in ip_list:
            if type == 'SSL':
                broker_string = broker_string + ',' + item + ':' + kafka_ssl_port
            elif type == 'PLAINTEXT':
                broker_string = broker_string + ',' + item + ':' + kafka_plaintext_port
        broker_string = broker_string[1:]
        broker_list = broker_string.split(',')
        logging.info("Kafka broker list updated " + str(broker_list))
        return broker_list

    def ConnectToKafka_SSL():
        kafka_brokers_list = get_Kafka_brokers_list('SSL')
        try:
            producer = KafkaProducer(bootstrap_servers=kafka_brokers_list,
                          security_protocol='SSL',
                          ssl_check_hostname=False,
                          ssl_cafile='/prometheus_exporter/certificates/ca.pem',
                          ssl_certfile='/prometheus_exporter/certificates/producer.pem',
                          ssl_keyfile='/prometheus_exporter/certificates/producer.key', max_request_size=5120000,
                          request_timeout_ms = 90000)
            return producer
        except Exception as e:
            error_string = str(e)
            logging.critical("Unable to connect to kafka server " + error_string)
            return False

    def ConnectToKafka_PLAINTEXT():
        kafka_brokers_list = get_Kafka_brokers_list('PLAINTEXT')
        try:
            producer = KafkaProducer(bootstrap_servers=kafka_brokers_list)
            return producer
        except Exception as e:
            error_string = str(e)
            logging.critical("Unable to connect to kafka server " + error_string)
            return False

    def ConnectToKafka():
        if secureConnectionToKafka == True:
            producer=ConnectToKafka_SSL()
        else:
            producer=ConnectToKafka_PLAINTEXT()
        return producer

    def run(self):
        producer = ConnectToKafka()
#        producer = KafkaProducer(bootstrap_servers=['35.182.138.92:9092','35.182.156.243:9092'])

        while not self.stop_event.is_set():
            producer.send('my-topic', b"test")
            producer.send('my-topic', b"\xc2Hola, mundo!")
            time.sleep(1)

        producer.close()

class Consumer(multiprocessing.Process):
    def __init__(self):
        multiprocessing.Process.__init__(self)
        self.stop_event = multiprocessing.Event()

    def stop(self):
        self.stop_event.set()

    def run(self):
        consumer = KafkaConsumer(bootstrap_servers=['35.182.138.92:9092','35.182.156.243:9092'],
                                 auto_offset_reset='earliest',
                                 consumer_timeout_ms=1000)
        consumer.subscribe(['my-topic'])

        while not self.stop_event.is_set():
            for message in consumer:
                print(message)
                if self.stop_event.is_set():
                    break

        consumer.close()


def main():
    tasks = [
        Producer(),
        Consumer()
    ]

    for t in tasks:
        t.start()

    time.sleep(10)

    for task in tasks:
        task.stop()

    for task in tasks:
        task.join()


if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO
        )
    main()
