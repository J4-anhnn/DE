import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import java.util.Properties;

public class ClickStreamJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "kafka:9092");
        props.setProperty("group.id", "click-group");

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
                "clickstream", new SimpleStringSchema(), props
        );

        env.addSource(consumer)
            .map(value -> "Processed: " + value)
            .print();

        env.execute("Click Stream Processing Job");
    }
}
