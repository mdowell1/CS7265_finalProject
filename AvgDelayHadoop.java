package proj;

import au.com.bytecode.opencsv.CSVParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class AvgDelayHadoop {

    private static final String FROM = "lax";

    public static void main(String[] args) throws Exception {
        long start = System.nanoTime(); // time before job runs

        // Create configuration, job, and set input/output locations
        Configuration conf = new Configuration();
        Job job = new Job(conf, "Average departure dealy calculator from given airport");
        String input = "hdfs://localhost:9000/test";
        String output = "hdfs://localhost:9000/output/outHadoop";

        // set needed options for MapReduce job
        job.setJarByClass(AvgDelayHadoop.class);
        job.setMapperClass(DelayMapper.class);
        job.setReducerClass(DelayReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        boolean finish = job.waitForCompletion(true); //get output of job running - true is to set verbose
        long end = System.nanoTime(); // get time after job finished
        long elapsedTime = end - start; // get total time to complete job in nanoseconds
        double elapsedTimeInSecond = (double) elapsedTime / 1_000_000_000; // convert time from nanoseconds to seconds
        System.out.println(elapsedTimeInSecond + " seconds");
        System.exit(finish ? 0 : 1);
    }

    // Counter for holding delay and flights for all files - keeping this in
    // makes the last line of the output file a full average
    public enum HadoopCounter {
        TotalDelay,
        TotalFlights
    }

    // mapper - reads input and writes KVPs for each valid line
    public static class DelayMapper extends Mapper<Object, Text, Text, Text> {

        // CSVParser for reading each file
        private CSVParser csvParser = new CSVParser(',', '"');

        // does the mapping
        public void map(Object offset, Text value, Context context)
                throws IOException, InterruptedException {

            // get each line in csv
            String[] line = this.csvParser.parseLine(value.toString());

            // if line can be used
            if (line.length > 0 && isValid(line)) {
                context.write(new Text(line[0].toLowerCase()), new Text(line[15])); // write KVP (date, delay)
            }
        }

        // returns true if line is not empty, not cancelled or diverted, and is from correct airport
        private boolean isValid(String[] line) {

            // if line is empty, do not use
            if (line == null || line.length == 0) {
                return false;
            }

            // if flight was cancelled (21) or diverted (23), do not use
            if (line[21].equals("1") || line[23].equals("1")) {
                return false;
            }

            // use this line if origin is the provided airport
            if (line[16].toLowerCase().equals(FROM)) {
                return true;
            }

            return false;
        }
    }


    // reduces KVPs to get delay, flights, and average delay
    public static class DelayReducer extends Reducer<Text, Text, Text, Text> {
        private CSVParser csvParser;
        private int totalFlights;
        private float totalDelay;

        // reducer setup to initialize variables
        protected void setup(Context context) {
            this.csvParser = new CSVParser(',', '"');
            this.totalDelay = 0;
            this.totalFlights = 0;
        }

        // reduce called for all KVPs - gets and outputs average
        public void reduce(Text key, Iterable<Text> values,
                           Context context) throws IOException, InterruptedException {

            // loop through each map line
            for (Text value : values) {
                String[] flightData = this.csvParser.parseLine(value.toString()); // get the flight delay
                float delay = Float.parseFloat(flightData[0]); // change delay to float value
                this.totalDelay += delay; // increase delay and flight num
                this.totalFlights++;
            }

            // don't use these 2 lines if you want average for each file rather than all files
            context.getCounter(HadoopCounter.TotalDelay).increment((long) totalDelay);
            context.getCounter(HadoopCounter.TotalFlights).increment(totalFlights);
            double finalTotal = context.getCounter(HadoopCounter.TotalDelay).getValue(); // total delay number
            double finalCount = context.getCounter(HadoopCounter.TotalFlights).getValue(); // total flight number
            double avg = finalTotal / finalCount;
            context.write(new Text("Average"), new Text(String.valueOf(avg))); // output the average
        }
    }
}