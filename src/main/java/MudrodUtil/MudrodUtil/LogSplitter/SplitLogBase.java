package MudrodUtil.MudrodUtil.LogSplitter;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;


public class SplitLogBase implements Serializable {
	
	public SplitLogBase(){
		
	}
	
	public List<String> splitLogs(JavaSparkContext sc, String httpfile, String ftpfile, int num){
		return null;
	}
	
	public List<String> splitLogs(String logpath, int num){
		return null;
	}
	
	public List<String> saveSplitToTxtFile(JavaPairRDD<Integer, WebLog> groupLogs, int num, String httpfile, String ftpfile){
		List<String> fileList = new ArrayList<String>();
		
		JavaRDD<WebLog> splitLogRDD = groupLogs.partitionBy(new HashPartitioner(num)).values();

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
}
