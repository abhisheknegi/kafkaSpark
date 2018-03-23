package util;

import java.util.HashMap;
import java.util.Map;

import scala.Serializable;

@SuppressWarnings("serial")
public class KafkaManager implements Serializable {
	
	public Map<String, String> getKafkaProp(Config Config){

		Map<String, String> broker_list = new HashMap<>();
		broker_list.put("KB1", "35.171.25.29:9092");
		broker_list.put("KB2", "35.171.25.30:9092");
		
		Map<String, String> prop = new HashMap<>();
		prop.put("bootstrap.servers", broker_list.get(Config.getkafkaBroker()));
		
		Config.setkafkaBroker(broker_list.get(Config.getkafkaBroker()));
		
		return prop;
	
	}
	
}
