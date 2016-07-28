package MudrodUtil.MudrodUtil.Util;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkNode implements Serializable  {

	private static final long serialVersionUID = 1L;
	public SparkConf conf;
	public JavaSparkContext sc;
	
	public SparkNode(Map<String,String> config) throws IOException{
		 conf = new SparkConf()
				.setMaster(config.get("sparkMaster"))
				.set("spark.driver.port",config.get("spark.driver.port"))
				.set("spark.executor.port",config.get("spark.executor.port"))
	            .setAppName("Indexer")
	            .set("spark.hadoop.validateOutputSpecs", "false")
	            .set("spark.executor.memory",config.get("spark.executor.memory"));

		sc = new JavaSparkContext(conf);
		
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);
	}
}
