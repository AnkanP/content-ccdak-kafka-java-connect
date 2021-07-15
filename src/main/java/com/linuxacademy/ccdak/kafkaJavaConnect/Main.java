package com.linuxacademy.ccdak.kafkaJavaConnect;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class Main {

    public static void main(String[] args) {

        //java properties object for properties class
        Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"172.31.105.172:9092");
        props.put(ProducerConfig.ACKS_CONFIG,"all");
        props.put(ProducerConfig.RETRIES_CONFIG,0);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        //kafka producer object takes in 2 parameters key & value
        Producer<String, String> producer = new KafkaProducer<String, String>(props);

        for(int i =300; i< 400; i++){
            int partition =0;
            if(i > 350) {
                partition =1;
            }
            //Producer record is class to create a producer record which needs to basically match 3 fields topic-name,key,value

            ProducerRecord record = new ProducerRecord("test_count",partition,"count",Integer.toString(i));

            producer.send(record, (RecordMetadata metadata , Exception e) ->
            {
                if (e != null) {
                    System.out.println("Error publishing message: " + e.getMessage());
                } else {
                    System.out.println("Published message: key=" + record.key() +
                            ", value=" + record.value() +
                            ", topic=" + metadata.topic() +
                            ", partition=" + metadata.partition() +
                            ", offset=" + metadata.offset());
                }
            }

        );
        }
        producer.close();

    }
}
