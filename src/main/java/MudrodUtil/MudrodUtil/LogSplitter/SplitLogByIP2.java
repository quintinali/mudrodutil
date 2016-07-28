package MudrodUtil.MudrodUtil.LogSplitter;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import com.google.common.base.Optional;

import MudrodUtil.MudrodUtil.PartitionLib.KGreedyPartitionSolver;
import MudrodUtil.MudrodUtil.PartitionLib.LinearPartitionSolver;
import MudrodUtil.MudrodUtil.PartitionLib.Partition;
import MudrodUtil.MudrodUtil.PartitionLib.ThePartitionProblemSolver;
import scala.Tuple2;

public class SplitLogByIP2 extends SplitLogBase{

	public SplitLogByIP2() {
		// TODO Auto-generated constructor stub
	}
	
	public List<String> splitLogs(JavaSparkContext sc, String httpfile,String ftpfile, int num) {
		List<List<String>> splitFiles = new ArrayList<List<String>>();

		JavaRDD<WebLog> accessLogs = sc.textFile(httpfile).map(s -> ApacheAccessLog.parseFromLogLine(s)).filter(WebLog::checknull);
		JavaPairRDD<String, Double> IPhttpLineRDD = this.calIPLoglines(accessLogs);
		
		JavaRDD<WebLog> ftpLogs = sc.textFile(ftpfile).map(s -> FtpLog.parseFromLogLine(s)).filter(WebLog::checknull);
		JavaPairRDD<String, Double> IPftpLineRDD = this.calIPLoglines(ftpLogs);
		
		JavaPairRDD<String, Double> IPLineRDD = IPhttpLineRDD.fullOuterJoin(IPftpLineRDD).mapToPair(new PairFunction<Tuple2<String, Tuple2<Optional<Double>, Optional<Double>>>, String, Double>() {
			@Override
			public Tuple2<String, Double> call(Tuple2<String, Tuple2<Optional<Double>, Optional<Double>>> arg0)
					throws Exception {
				// TODO Auto-generated method stub
				Optional<Double> ftplines = arg0._2._2;
				Optional<Double> httplines = arg0._2._1;
				Double lines = 0.0;
				if(ftplines.isPresent()){
					lines += ftplines.get();
				}
				if(httplines.isPresent()){
					lines += httplines.get();
				}
				return new Tuple2<String, Double>(arg0._1, lines);
			}
		});
		
		Map<String, Integer> IpGroups = this.groupIPs(IPLineRDD, num);
		//this.splitAllLog(accessLogs.union(ftpLogs), IpGroups, num, httpfile);
		//this.splitAllLog(ftpLogs, IpGroups, num, ftpfile);
		return this.splitAllLog(accessLogs.union(ftpLogs), IpGroups, num, httpfile,ftpfile);
	}
	
	
	//for test
	private List<String> splitAllLog(JavaRDD<WebLog> logs, Map<String, Integer> IpGroups,int num, String file){
		
		JavaPairRDD<Integer, String> groupLogs = logs.mapToPair(new PairFunction<WebLog, Integer, String>() {
			@Override
			public Tuple2<Integer, String> call(WebLog log) throws Exception {
				// TODO Auto-generated method stub
				String IP = log.IP;
				Integer groupId = IpGroups.get(IP);
				return new Tuple2<Integer, String>(groupId, log._source);
			}
		});
		

		groupLogs.partitionBy(new HashPartitioner(num)).values().saveAsTextFile(file + "_n");

		return null;
	}

	
	private List<String> splitAllLog(JavaRDD<WebLog> logs, Map<String, Integer> IpGroups,int num, String httpfile,  String ftpfile){
		
		JavaPairRDD<Integer, WebLog> groupLogs = logs.mapToPair(new PairFunction<WebLog, Integer, WebLog>() {
			@Override
			public Tuple2<Integer, WebLog> call(WebLog log) throws Exception {
				// TODO Auto-generated method stub
				String IP = log.IP;
				Integer groupId = IpGroups.get(IP);
				return new Tuple2<Integer, WebLog>(groupId, log);
			}
		});
	
		List<String> fileList = this.saveSplitToTxtFile(groupLogs, num, httpfile, ftpfile);

		return fileList;
	}
	
	private JavaPairRDD<String, Double> calIPLoglines(JavaRDD<WebLog> logs){
		JavaPairRDD<String,Double> ipLineRDD = logs.mapToPair(new PairFunction<WebLog, String, Double>() {
			@Override
			public Tuple2<String, Double> call(WebLog arg0) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2<String, Double>(arg0.IP, 1.0);
			}
		}).reduceByKey(new Function2<Double, Double, Double>() {
			public Double call(Double first, Double second) throws Exception {
				return first + second;
			}
		});
		
		return ipLineRDD;
	}
	
	private Map<String, Integer>  groupIPs(JavaPairRDD<String, Double> IPLineRDD, int num){
		
		JavaPairRDD<Double,String> lineIPRDD = IPLineRDD.mapToPair(new PairFunction<Tuple2<String, Double>, Double, String>() {
	           @Override
	           public Tuple2<Double, String> call(Tuple2<String, Double> item) throws Exception {
	               return item.swap();
	           }

	     }).sortByKey(false);
		
		List<Tuple2<Double,String>> userlines = lineIPRDD.collect();
		Map<String, Double> user_lines_map = new LinkedHashMap<String, Double>();
		int usernum = userlines.size();
		for(int i=0; i<usernum; i++){
			user_lines_map.put(userlines.get(i)._2, userlines.get(i)._1);
		}
		
		//System.out.println(user_lines_map);

		ThePartitionProblemSolver solution = new KGreedyPartitionSolver(true);
		Map<String, Integer> IpGroups = solution.solve(user_lines_map, num);
		
		//System.out.println(IpGroups);
		
		return IpGroups;
	}
}
	

