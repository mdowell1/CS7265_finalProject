import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.Map;

//This class creates the KafkaPrintBolt. It filters tuples based on the airport code and prints the year at the end of the file
public class KafkaPrintBolt extends BaseRichBolt {

    private OutputCollector collector;
    String airport_to_calculate = "SAN"; //This sets the airport code to filter for.
    String year; //variable to hold the year
    Integer delay; //variable to hold departure delay
    String airport; //variable to hold airport code
    String fileName = "/Users/matt/Downloads/kafka-storm-output.txt"; //use to set location of output file

    @Override
    public void prepare(Map<String, Object> conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }
    @Override
    public void execute(Tuple tuple) {
        String csvRecord = tuple.getStringByField("value"); //gets the rows of the csv file that are sent as tuples from Kafka Spout
        if ("__END_OF_FILE__".equals(csvRecord)) { //detects end of a file and writes the year if so
            System.out.println("End of File");
            try {
                // Create a FileWriter and a BufferedWriter
                FileWriter fileWriter = null;
                try {
                    fileWriter = new FileWriter(fileName, true);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);

                // Write the content to the file
                bufferedWriter.write("Year: " + year + System.lineSeparator());

                // Close the BufferedWriter
                bufferedWriter.close();

            } catch (IOException e) {
                System.err.println("Error writing to file: " + e.getMessage());
            }
            collector.emit(new Values("EOF", -1)); //emits a tuple denoting the end of the file to downstream bolts
            collector.ack(tuple); //tells kafka the tuple has been processed
        } else {
            List<String> columns = Arrays.asList(csvRecord.split(",")); //splits the columns of the csv row into list
            if (columns.get(16).equals(airport_to_calculate)) { //checks if the airport code is the one being searched for
                if (!columns.get(15).equals("NA") && !columns.get(23).equals("1")){ //filters out flights that were cancelled or diverted
                    year = columns.get(0); // gets index of year
                    delay = Integer.valueOf(columns.get(15)); // gets index of departure delay
                    airport = columns.get(16); // gets index of airport code
                    collector.emit(new Values(airport, delay)); //emits airport code and departure delay to next bolt
                    collector.ack(tuple); //tells kafka the tuple has been processed
                }
                collector.ack(tuple); //tells kafka the tuple has been processed
            }
            collector.ack(tuple); //tells kafka the tuple has been processed
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("airport", "delay"));
    }
}

