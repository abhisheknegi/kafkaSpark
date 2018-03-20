package kafkaSpark;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class SparkStreamConsumer {

	public static void main(String[] args) {
		
		SparkConf sconf = new SparkConf().setAppName("SparkKafka").setMaster("local[*]");
		
		JavaStreamingContext jssc = new JavaStreamingContext(sconf, Durations.seconds(2));
		
	    jssc.start();
	    
	    try {
			jssc.awaitTermination();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	    jssc.close();

	}

}
