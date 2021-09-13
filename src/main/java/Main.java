import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class Main {
    public static void main(String[] args) {
        Properties props = new Properties();
        // Docker container id if not running from docker container in the network, otherwise container name
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("linger.ms", 1);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new KafkaProducer<>(props);
        for (int i = 0; i < 10000; i++) {
            long data = (long) (Math.random() * (100));
            // .get makes send() blocking, allows to analyse send back Metadata (timestamp used by Kafka broker)
            producer.send(new ProducerRecord<String, String>("mytopic", 0, (Integer.toString(i)), "{\"venue\":{\"country\": \"DE\", \"city\": \"Munich\" }, \"sensordata\":\"" + data + "\"}"));//.get().timestamp();
            System.out.println("i:" + i + "partion 0" + " data: " + data);
            i++;
            long data2 = (long) (Math.random() * (100));
            producer.send(new ProducerRecord<String, String>("mytopic", 1, Integer.toString(i), "{\"venue\":{\"country\": \"FR\", \"city\": \"Paris\" }, \"sensordata\":\"" + data2 + "\"}"));
            System.out.println("i:" + i + "partion 1" + "data: " + data2);
        }
        producer.close();
    }
}


//The result of the send is a RecordMetadata specifying the partition the record was sent to, the offset it was assigned and the timestamp of the record. If CreateTime is used by the topic, the timestamp will be the user provided timestamp or the record send time if the user did not specify a timestamp for the record. If LogAppendTime is used for the topic, the timestamp will be the Kafka broker local time when the message is appended.
//https://kafka.apache.org/28/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html