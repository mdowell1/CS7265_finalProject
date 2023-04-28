import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

public class SumAndCountBolt extends BaseRichBolt {
    private OutputCollector collector;
    private Map<String, Integer> airportDelaySum; // map to hold departure delay sums for airports
    private Map<String, Integer> airportDelayCount; // map to hold counts for airport departure delays

    @Override
    public void prepare(Map<String, Object> conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.airportDelaySum = new HashMap<>();
        this.airportDelayCount = new HashMap<>();
    }

    @Override
    public void execute(Tuple tuple) {

        String airport = tuple.getStringByField("airport"); // gets the airport code that was emitted from previous bolt
        int delay = tuple.getIntegerByField("delay"); // gets the departure delay that was emitted from previous bolt

        if ("EOF".equals(airport)) { // checks for the end of a file
            // Pass the "end of file" tuple to the next bolt
            collector.emit(new Values("EOF", -1, -1)); // emits an end of file message to the downstream bolt
        } else {
            airportDelaySum.put(airport, airportDelaySum.getOrDefault(airport, 0) + delay); // gets current delay sum for airport and adds incoming delay
            airportDelayCount.put(airport, airportDelayCount.getOrDefault(airport, 0) + 1); // gets current count for airport and adds one

            collector.emit(new Values(airport, airportDelaySum.get(airport), airportDelayCount.get(airport))); //emits airport code, delay sum, and delay count to next bolt
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("airport", "sum", "count"));
    }
}

