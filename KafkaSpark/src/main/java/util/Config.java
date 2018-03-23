package util;

import java.io.Serializable;

import org.apache.spark.streaming.Duration;

@SuppressWarnings("serial")
public class Config implements Serializable {	
	
	public String Bootstrap;
	public String topic;
	public Duration Duration;
	public String SparkMaster;
	public String AppName;
	
	public String getkafkaBroker() {
		return this.Bootstrap; 
	}
	
	public void setkafkaBroker(String Bootstrap) {
		this.Bootstrap = Bootstrap;
	}
	
	public String getkafkatopic() {
		return this.topic; 
	}
	
	public void setkafkatopic(String Topic) {
		this.topic = Topic;
	}
	
	public org.apache.spark.streaming.Duration getbatchduration() {
		return this.Duration;
	}
	
	public void setbatchduration(int batchduration) {
		this.Duration = new Duration(batchduration);
	}
	
	public String getSparkMaster() {
		if("".equals(this.SparkMaster)) {
			return "local[2]";
		}else {
			return this.SparkMaster;
		}
		 
	}
	
	public void setSparkMaster(String SparkMaster) {
		if("local".equalsIgnoreCase(SparkMaster)) {
			this.SparkMaster = "local[2]";
		}else {
			this.SparkMaster = SparkMaster;
		}
	}
	
	public String getAppName() {
		return this.AppName; 
	}
	
	public void setAppName(String AppName) {
		this.AppName = AppName;
	}
	
}
