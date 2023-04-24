import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import static org.apache.spark.sql.functions.avg;


public class AvgDelaySpark {
    private static final String FROM = "LAX";
    static String input = "hdfs://localhost:9000/test";
    static String output = "hdfs://localhost:9000/output/outSpark";

    public static void main(String[] args) throws Exception {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);
        long start = System.nanoTime(); // time before job runs

        // Create Spark Session
        SparkSession spark = SparkSession
                .builder()
                .config("spark.master", "local")
                .appName("AvgDelay")
                .getOrCreate();

        // Creates a dataset from the csv files in input
        Dataset<Row> df = spark.read().option("header", "true").csv(input);

        // Narrows dataset to only include ones from given origin that were not cancelled or diverted
        df = df.filter((df.col("Cancelled").contains("0"))
                .and(df.col("Diverted").contains("0"))
                .and(df.col("Origin").contains(FROM)));

        //  Changes dataset to show the average delay per year
        //   Dataset<Row> df2 = df.groupBy("Year").agg(avg(df.col("DepDelay")).as("AverageDelay"));

        // Changes dataset to show the average delay for all years
        Dataset<Row> df3 = df.groupBy("Origin").agg(avg(df.col("DepDelay")).as("AverageDelay"));

        // Create file with delay info
        df3.coalesce(1)
                .write()
                .option("header", false)
                .csv(output);

        long end = System.nanoTime(); // get time after job finished
        long elapsedTime = end - start; // get total time to complete job in nanoseconds
        double elapsedTimeInSecond = (double) elapsedTime / 1_000_000_000; // convert time from nanoseconds to seconds
        System.out.println(elapsedTimeInSecond + " seconds");
    }
}