import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AverageDelayBolt extends BaseRichBolt {
    private HashMap<String, List<Integer>> airportDataMap; // map to hold delay sum and count for airports

    @Override
    public void prepare(Map<String, Object> conf, TopologyContext context, OutputCollector collector) {
        this.airportDataMap = new HashMap<>();
    }

    @Override
    public void execute(Tuple tuple) {

        String airport = tuple.getStringByField("airport"); // gets airport code from previous bolt
        int sum = tuple.getIntegerByField("sum"); // gets delay sum from previous bolt
        int count = tuple.getIntegerByField("count"); // gets count from previous bolt

        if ("EOF".equals(airport)) { // checks for end of file

            // this loop goes through each airport and calculates average departure delay
            for (Map.Entry<String, List<Integer>> entry : airportDataMap.entrySet()) {
                String temp_airport = entry.getKey(); // gets airport code
                List<Integer> data = entry.getValue(); // gets list of data for the airport code
                int temp_sum = data.get(0); // gets the delay sum for the airport
                int temp_count = data.get(1); // gets the count for the airport
                double average = (double) temp_sum / temp_count; // calculates the average delay for the airport

                // this section writes the airport code, average departure delay, count, sum, and end time for each airport/file
                String fileName = "/Users/matt/Downloads/kafka-storm-output.txt";

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
                    bufferedWriter.write("Airport:" + temp_airport + " Avg Dep. Delay: " + average + " count:" + temp_count + " sum:" + temp_sum  + System.lineSeparator());
                    bufferedWriter.write("End time for this file: " + String.valueOf(System.nanoTime()) + System.lineSeparator());

                    // Close the BufferedWriter
                    bufferedWriter.close();
                } catch (IOException e) {
                    System.err.println("Error writing to file: " + e.getMessage());
                }
            }

        } else {
            // if its not the end of the file this section either writes the new airport code and data to a list, or if it exists already updates it
            if (airportDataMap.containsKey(airport)) { //checks if the airport is already in the map
                // Key exists, update the sum and count
                List<Integer> data = airportDataMap.get(airport); // stores the data for the airport
                data.set(0, sum);   // Updates the sum
                data.set(1, count); // Updates the count
            } else {
                // If airport does not exist, this adds a new entry
                List<Integer> data = new ArrayList<>();
                data.add(sum);   // Add the sum
                data.add(count); // Add the count

                airportDataMap.put(airport, data); //puts the airport and data into the map
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // No need to declare output fields since we're not emitting any tuple
    }
}

