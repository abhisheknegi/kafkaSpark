package kafkaSpark;

import util.Config;

public class ExecutorApp {

	public static void main(String[] args) {
		
		Config Config = new Config();
		Config.setkafkaBroker("KB1");
		Config.setkafkatopic("testTopic");
		Config.setbatchduration(1000);
		Config.setSparkMaster("local");
		Config.setAppName("Spark-Kafka-Streaming");
		
		SparkStreamConsumer SSC = new SparkStreamConsumer();
		
		try {
			SSC.SparkInitiator(Config);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	}

}
