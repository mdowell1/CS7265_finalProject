import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

//This class creates the topology and the Kafka Spout for consuming data from the Kafka server
public class KafkaStormTopology {

    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("kafka-spout", createKafkaSpout());  //creates the spout that connects to kafka to consume data
        builder.setBolt("kafkaPrintBolt", new KafkaPrintBolt()).shuffleGrouping("kafka-spout"); //connects the PrintBolt to the spout
        builder.setBolt("sumAndCountBolt", new SumAndCountBolt()).shuffleGrouping("kafkaPrintBolt"); //connects the SumAndCount bolt
        builder.setBolt("averageDelayBolt", new AverageDelayBolt()).shuffleGrouping("sumAndCountBolt"); //connects the AverageDelay bolt

        Config config = new Config();
        config.put("topology.spout.max.batch.size", 64*1024); //increased the batch size to attempt to improve performance
//        config.setNumWorkers(4); //tested increasing the number of JVMs to speed up system; No improvement
        config.setDebug(true);
        String topologyName = "kafka-storm-topology";

        try (LocalCluster cluster = new LocalCluster()) {
            cluster.submitTopology(topologyName, config, builder.createTopology());
            // Let the topology run for some time (e.g., 1 minute)
            Thread.sleep(6_000_000); // This sets a time at which the cluster will shutdown. Otherwise it will continue to run.
            cluster.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    //This is the Kafka Spout which will consume data from the kafka server
    private static KafkaSpout<String, String> createKafkaSpout() {
        String bootstrapServers = "localhost:9092"; // Kafka broker address
        String topic = "airline_data"; // Kafka topic to be ingested

        KafkaSpoutConfig.Builder<String, String> spoutConfigBuilder = KafkaSpoutConfig.builder(bootstrapServers, topic);
        spoutConfigBuilder.setProp(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        spoutConfigBuilder.setProp(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        spoutConfigBuilder.setProp(ConsumerConfig.GROUP_ID_CONFIG, "storm-kafka-spout-group");
        spoutConfigBuilder.setProcessingGuarantee(KafkaSpoutConfig.ProcessingGuarantee.AT_LEAST_ONCE);
//        spoutConfigBuilder.setProp(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        spoutConfigBuilder.setOffsetCommitPeriodMs(5000); //set this to try and speed up the system by slowing down commits; No noticeable improvement
        spoutConfigBuilder.setRecordTranslator((r) -> new Values(r.key(), r.value()), new Fields("key", "value"));

        KafkaSpoutConfig<String, String> spoutConfig = spoutConfigBuilder.build();

        //This section writes the start time to a file
        String fileName = "/Users/matt/Downloads/kafka-storm-output.txt";

        try {
            // Create a FileWriter and a BufferedWriter
            FileWriter fileWriter = null;
            try {
                fileWriter = new FileWriter(fileName);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);

            // Write the content to the file
            bufferedWriter.write("Start Time: " + String.valueOf(System.nanoTime()) + System.lineSeparator());

            // Close the BufferedWriter
            bufferedWriter.close();
        } catch (IOException e) {
            System.err.println("Error writing to file: " + e.getMessage());
        }

        return new KafkaSpout<>(spoutConfig);
    }
}

