from cargo.src.kafka_manager.kafka_manager import KafkaAvroProcessor


conf = {
    "bootstrap.servers": "kafka1.m1test.eip.s7.aero:9093,kafka2.m1test.eip.s7.aero:9093,kafka3.m1test.eip.s7.aero:9093",
    "ssl.ca.location": "/home/steel/work/cargo-library/new/CARoot.pem",
    "ssl.certificate.location": "/home/steel/work/cargo-library/new/certificate.pem",
    "ssl.key.location": "/home/steel/work/cargo-library/new/key.pem",
    "group.id": 'cargoai6',
    "security.protocol": "SSL"
}

c = KafkaAvroProcessor(conf)
c.init_consumer(schema_registry_url="https://ksr-eip-rtc.s7.aero", topics=['FDW_FLIGHT_EVENT_UAT', 'FDW_LEG_EVENT_UAT'])
c.consume()
