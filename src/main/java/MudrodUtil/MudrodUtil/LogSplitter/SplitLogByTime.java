package MudrodUtil.MudrodUtil.LogSplitter;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.HashPartitioner;
import org.apache.spark.Partition;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import com.google.common.base.Optional;

import MudrodUtil.MudrodUtil.PartitionLib.LinearPartitionSolver;
import MudrodUtil.MudrodUtil.PartitionLib.ThePartitionProblemSolver;
import scala.Tuple2;

public class SplitLogByTime extends SplitLogBase{

	public SplitLogByTime() {
		// TODO Auto-generated constructor stub
	}
	
	public List<String> splitLogs(JavaSparkContext sc, String httpfile, String ftpfile, int num) {

		System.out.println(httpfile);
		System.out.println(ftpfile);
		
		JavaRDD<WebLog> accessLogs = sc.textFile(httpfile).map(s -> ApacheAccessLog.parseFromLogLine(s)).filter(WebLog::checknull);
		JavaPairRDD<Integer, Double> httpDayLineRDD = this.calDailyLoglines(accessLogs);
	
		JavaRDD<WebLog> ftpLogs = sc.textFile(ftpfile).map(s -> FtpLog.parseFromLogLine(s)).filter(WebLog::checknull);
		JavaPairRDD<Integer, Double> ftpDayLineRDD = this.calDailyLoglines(ftpLogs);
		
		JavaPairRDD<Integer, Double> dailyTotalLineRDD = httpDayLineRDD.leftOuterJoin(ftpDayLineRDD).mapToPair(new PairFunction<Tuple2<Integer, Tuple2<Double, Optional<Double>>>, Integer, Double>() {
			@Override
			public Tuple2<Integer, Double> call(Tuple2<Integer, Tuple2<Double, Optional<Double>>> arg0) throws Exception {
				Optional<Double> ftplines = arg0._2._2;
				Double httplines = arg0._2._1;
				Double lines = httplines;
				if(ftplines.isPresent()){
					lines += ftplines.get();
				}
				return new Tuple2<Integer, Double>(arg0._1, lines);
			}
		}).sortByKey(true);
		
		Map<Integer, Integer> dayGroups = this.groupDays(dailyTotalLineRDD, num);
		return this.splitAllLog(accessLogs.union(ftpLogs), dayGroups, num, httpfile,ftpfile);
	}
	
	private JavaPairRDD<Integer, Double> calDailyLoglines(JavaRDD<WebLog> logs){
		JavaPairRDD<Integer, Double> dayLineRDD = logs.mapToPair(new PairFunction<WebLog, Integer, Double>() {
			@Override
			public Tuple2<Integer, Double> call(WebLog arg0) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2<Integer, Double>(arg0.day, 1.0);
			}
		}).reduceByKey(new Function2<Double, Double, Double>() {
			public Double call(Double first, Double second) throws Exception {
				return first + second;
			}
		});
		
		return dayLineRDD;
	}
	
	private Map<Integer, Integer>  groupDays(JavaPairRDD<Integer, Double> dailyTotalLineRDD, int num ){
		
		List<Integer> days = dailyTotalLineRDD.keys().collect();
		List<Double> lines = dailyTotalLineRDD.values().collect();

		ThePartitionProblemSolver solution = new LinearPartitionSolver();
		double[] lineArr = new double[lines.size()];
		for(int i=0; i<lines.size(); i++){
			lineArr[i] = lines.get(i);
		}
		int[] dividers = solution.solve(lineArr, num);
		System.out.println("divided to: "  + num + ",diveders:");
		for (int i = 0; i < dividers.length; i++) {
			System.out.println(dividers[i]);
		}
		
		int daysNum = days.size();
		int group = 0;
		Map<Integer, Integer> dayGroups = new HashMap<Integer, Integer>();
		for(int i=0; i<daysNum; i++){
			if (dividers.length > group) {
				int divider = dividers[group];
				if (i < divider) {
					dayGroups.put(days.get(i), group);
				} else {
					group += 1;
					dayGroups.put(days.get(i), group);
				}
			}else{
				dayGroups.put(days.get(i), group);
			}
		}
		
		return dayGroups;
	}
	
	private List<String> splitAllLog(JavaRDD<WebLog> logs, Map<Integer, Integer> dayGroups,int num, String httpfile,  String ftpfile){
		
		List<String> fileList = new ArrayList<String>();
		
		JavaPairRDD<Integer, WebLog> groupHttpLogs = logs.mapToPair(new PairFunction<WebLog, Integer, WebLog>() {
			@Override
			public Tuple2<Integer, WebLog> call(WebLog log) throws Exception {
				// TODO Auto-generated method stub
				Integer day = log.day;
				Integer groupId = dayGroups.get(day);
				return new Tuple2<Integer, WebLog>(groupId, log);
			}
		});
		
		JavaRDD<WebLog> splitLogRDD = groupHttpLogs.partitionBy(new HashPartitioner(num)).values();

		String http_part_file = httpfile + "_part";
		String ftp_part_file = ftpfile + "_part";
		
		fileList.add(http_part_file);
		fileList.add(ftp_part_file);
		
		splitLogRDD.foreachPartition(new VoidFunction<Iterator<WebLog>>(){
			@Override
			public void call(Iterator<WebLog> arg0) throws Exception {
				// TODO Auto-generated method stub
				FileWriter httpfw;
				BufferedWriter httpbw = null;
				
				FileWriter ftpfw;
				BufferedWriter ftpbw = null;
				
				try {
					
					httpfw = new FileWriter(new File(http_part_file));
					httpbw = new BufferedWriter(httpfw);
					
					ftpfw = new FileWriter(new File(ftp_part_file));
					ftpbw = new BufferedWriter(ftpfw);
					
					while (arg0.hasNext()) {
						WebLog log = arg0.next();
						if(log.LogType.equals("http")){
							httpbw.append(log._source + System.lineSeparator());
						}else if(log.LogType.equals("ftp")){
							ftpbw.append(log._source + System.lineSeparator());
						}
					}
				}
				catch (IOException ioe) {
					throw new RuntimeException(ioe);
				}
				finally {
					if (httpbw != null) {
						httpbw.close();
					}
					
					if (ftpbw != null) {
						ftpbw.close();
					}
				}
			}
		});

		return fileList;
	}

	/*private List<String> splitLog(JavaRDD<WebLog> logs, Map<Integer, Integer> dayGroups,int num, String file){
		
		List<String> fileList = new ArrayList<String>();
		JavaPairRDD<Integer, String> groupHttpLogs = logs.mapToPair(new PairFunction<WebLog, Integer, String>() {
			@Override
			public Tuple2<Integer, String> call(WebLog log) throws Exception {
				// TODO Auto-generated method stub
				Integer day = log.day;
				Integer groupId = dayGroups.get(day);
				return new Tuple2<Integer, String>(groupId, log._source);
			}
		});
		
		//method1
		groupHttpLogs.partitionBy(new HashPartitioner(num)).values().saveAsTextFile(file + "_d");
		
		for(int i=0; i<num; i++){
			fileList.add(file +  "_d");
		}
		
		//method2
		for(int i=0; i<num; i++){
			final int groupid = i;
			JavaPairRDD<Integer, String> groupLogRDD = groupHttpLogs.filter(new Function<Tuple2<Integer, String>, Boolean>() {
				@Override
				public Boolean call(Tuple2<Integer, String> arg0) throws Exception {
					if(arg0._1 == groupid){
						return true;
					}
					return false;
				}
			});
			
			//method2
			//fileList.add(file + i + "_d");
			//groupLogRDD.values().coalesce(1,true).saveAsTextFile(file + i + "_d");
			
			//method3
			fileList.add(file + i + "_d");
			List<String> loglines = groupLogRDD.values().collect();
			this.saveToFile(loglines,file + i + "_d");
		}	
		

		return fileList;
	}
	
	public void saveToFile(List<String> lines, String fileName) {
		
		try {
			File file = new File(fileName);
			if (file.exists()) {
				file.delete();
			}
			file.createNewFile();
			FileWriter fw = new FileWriter(file.getAbsoluteFile());
			BufferedWriter bw = new BufferedWriter(fw);

			int linenum = lines.size();
			for(int i=0; i<linenum; i++){
				bw.write(lines.get(i)+ "\n");
			}
			
			bw.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
*/
}
	

