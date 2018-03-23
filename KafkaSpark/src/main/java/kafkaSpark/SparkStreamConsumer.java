package kafkaSpark;

import java.io.Serializable;
import java.util.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.serializer.StringDecoder;
import scala.Tuple2;
import util.Config;
import util.KafkaManager;

@SuppressWarnings("serial")
public class SparkStreamConsumer implements Serializable {

	public void SparkInitiator(Config Config) throws InterruptedException {
		
		final Logger log = LoggerFactory.getLogger(SparkStreamConsumer.class);
		
		KafkaManager KM = new KafkaManager();
		Map<String, String> K_prop = KM.getKafkaProp(Config);
		
		String topics = Config.getkafkatopic();
		Set<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
		
		log.info("Topic: "+ topics);
		
		SparkConf sconf = new SparkConf()
				.setAppName("SparkKafka")
				.setMaster(Config.getSparkMaster());
		
		JavaStreamingContext jssc = new JavaStreamingContext(sconf, Config.getbatchduration());
		
		JavaPairInputDStream<String, String> stream = KafkaUtils.createDirectStream(
										jssc, 
										String.class, 
										String.class, 
										StringDecoder.class, 
										StringDecoder.class, 
										K_prop, 
										topicsSet);
											
		JavaDStream<String> words = stream.map(new Function<Tuple2<String, String>, String>() {
		            @Override
		            public String call(Tuple2<String, String> tuple2) {
		              return tuple2._2();
		            }
          })
		.filter(x -> !"RT".equals(x.split(" ")[0]))
		.filter(x -> (x.contains("media")));
		
		words.print();
		
		
	    jssc.start();
	    
	    log.info("Streaming Started ....");
		jssc.awaitTermination();
		
		jssc.stop(true, true);

	}
}
