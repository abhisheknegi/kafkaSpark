package kafkaSpark;

import java.util.*;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import kafka.serializer.StringDecoder;

public class SparkStreamConsumer {

	public static void main(String[] args) {
		
		Map<String, String> prop = new HashMap<>();
		prop.put("bootstrap.servers", "35.171.25.29:9092");
		
		String topic = "testTopic";
		Set<String> topicsSet = new HashSet<>(Arrays.asList(topic.split(",")));
		
		SparkConf sconf = new SparkConf().setAppName("SparkKafka").setMaster("spark://54.224.107.130:7077");
		
		JavaStreamingContext jssc = new JavaStreamingContext(sconf, Durations.seconds(1));
		
		JavaPairInputDStream<String, String> stream = KafkaUtils.createDirectStream(
				jssc, 
				String.class, 
				String.class, 
				StringDecoder.class, 
				StringDecoder.class, 
				prop, 
				topicsSet);
		
		JavaPairDStream<String, String> words = stream.filter(x -> x._2.contains("Stormy"));
		
		
		
		words.print();
		
	    jssc.start();
	    
	    try {
			jssc.awaitTermination();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	    jssc.close();

	}

}
