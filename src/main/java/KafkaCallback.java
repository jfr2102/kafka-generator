import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import java.io.IOException;
import java.nio.file.*;

public class KafkaCallback implements Callback {
    int index;
    int partition;
    String city;
    long data;

    public KafkaCallback(int index, int partition, String city, long data) {
        this.index = index;
        this.partition = partition;
        this.city = city;
        this.data = data;
    }

    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        if (exception != null) {
            exception.printStackTrace();
        } else {
            //System.out.println("index:" + index + "; partion:" + partition + "; city:" + city + "; data: " + data + "; kafkatimestamp:" + metadata.timestamp());
            String csv_line = index + "; " + partition + "; " + city + "; " + data + "; " + metadata.timestamp() + ";\n";
            try {
                Files.write(Paths.get("produceroutput.csv"),csv_line.getBytes(), StandardOpenOption.APPEND);
            } catch (IOException e) {
                //exception handling left as an exercise for the reader
            }


        }
    }
}
