package MudrodUtil.MudrodUtil;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.google.gson.JsonObject;

import MudrodUtil.MudrodUtil.Cluster.SshOperator;
import MudrodUtil.MudrodUtil.Cluster.jobUtil;
import MudrodUtil.MudrodUtil.Config.ConfigGenerator;
import MudrodUtil.MudrodUtil.LogSplitter.GroupLogBySize;
import MudrodUtil.MudrodUtil.LogSplitter.SplitLogBase;
import MudrodUtil.MudrodUtil.LogSplitter.SplitLogByIP;
import MudrodUtil.MudrodUtil.LogSplitter.SplitLogByTime;
import MudrodUtil.MudrodUtil.PartitionLib.KGreedyPartitionSolver;
import MudrodUtil.MudrodUtil.PartitionLib.Partition;
import MudrodUtil.MudrodUtil.PartitionLib.ThePartitionProblemSolver;
import MudrodUtil.MudrodUtil.Util.ESNodeClient;
import MudrodUtil.MudrodUtil.Util.SparkNode;



/**
 * Hello world!
 *
 */
public class MudrodToolApp 
{
	private Map<String,String> config;
	private SparkNode spark;
	private ESNodeClient esnode;
	private List<String> ips;
	private int vmnum;
	private String configFile;
	
	//for workflow
	long startTime; 
	long endTime; 
	long lastTime;
	String jobName;
	String workflowUrl = "http://localhost:8080/MudrodUtil/";

	public void init(String configFile){
		this.startTime=System.currentTimeMillis(); 
		this.configFile = configFile;
		ConfigGenerator gen = new ConfigGenerator();
		try {
			config = gen.loadConfig(configFile);
			spark = new SparkNode(config);
			esnode = new ESNodeClient(config);
			//get cluster node ips
			ips = esnode.getNodeIPs();
			vmnum = ips.size();
			jobName = "";
			this.addJob();
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void destory(){
		this.endTime = System.currentTimeMillis();
		this.lastTime = (endTime-startTime)/1000;
		System.out.println("splitting is done!" + "Time elapsed： "+ lastTime +"s");
		this.stopJob();
	}
	
	public void addJob() {
		 if(config.get("workflow") !=null && config.get("workflow").equals("true")){

			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MMM-dd-HH:mm:ss");
			Date resultdate = new Date(startTime);
			String sDate = sdf.format(resultdate);
			this.jobName = "job-" + sDate;
			config.put("jobname", jobName);
			String url = workflowUrl + "startJob?jobname=" + jobName + "&step=splitlog&nodeip=1";
			System.out.println(url);
			JsonObject retObj = jobUtil.invokeURL(url);
		}
	}
	
	public void stopJob() {
		if(config.get("workflow") !=null && config.get("workflow").equals("true")){
			String url = workflowUrl + "updateJobStep?jobname=" + jobName + "&step=splitlog&lapsetime=" + lastTime+ "&nodeip=1";
			System.out.println(url);
			jobUtil.invokeURL(url);
		}
	}

	public void preprocessData() {

		try {
			if (config.get("file").equals("1")) {
				this.processFileData();
			}else{
				this.ProcessDirectoryData();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void processFileData() throws IOException{
		
		String splitby = config.get("splitby");
		SplitLogBase splitlog = null;
		if(splitby == null){
			splitlog = new SplitLogByIP();
			splitby = "IP";
		}else if(splitby.equals("IP")){
			splitlog = new SplitLogByIP();
		}else if(splitby.equals("Time")){
			splitlog = new SplitLogByTime();
		}
		
		System.out.println(splitby);
		
		String strmonth = config.get("month");
		String[] months = strmonth.split(",");
		for(int i=0; i<months.length; i++){
			String HTTPfileName = config.get("accesslog") + "." + months[i];
			String FTPfileName = config.get("ftplog") + "." + months[i];
			List<String> splitfiles =splitlog.splitLogs(spark.sc,HTTPfileName, FTPfileName, vmnum);
		}

		//scp config file/ log file to each vm
		ConfigGenerator gen = new ConfigGenerator();
		Map<String, List<String>> ipfiles= gen.regenConfig(configFile,config, ips, splitby);
		this.scpConfigFiles(ipfiles);
	}
	
	public void ProcessDirectoryData() throws IOException{
		
		String stryear= config.get("year");
		String[] years = stryear.split(",");
		List<String> months = new ArrayList<String>();
 		for(int j=0; j<years.length; j++){
			String year = years[j];
			for(int i =1 ;i<13; i++)  //attention start from 2
			{ 
				String month = null;
				if(i<10){
					month = "0" + Integer.toString(i);
				}else{
					month = Integer.toString(i);
				}
				months.add(stryear + month);
			}
		}
		
		String splitby = config.get("splitby");
		if(splitby == null || splitby.equals("Size")){
			GroupLogBySize grouplog = new GroupLogBySize();
			String HTTPfileName = config.get("accesslog");
			String FTPfileName = config.get("ftplog");
			Map<String, Double> monthsize= grouplog.extractFileSize(months, HTTPfileName, FTPfileName);
			Map<String, Integer> monthGroups = grouplog.groupLogs(monthsize, this.vmnum);
			//scp config file to each vm
			ConfigGenerator gen = new ConfigGenerator();
			Map<String, List<String>> ipfiles= gen.regenConfig(configFile,config, ips,monthGroups);
			this.scpConfigFiles(ipfiles);
		}else if(splitby.equals("IP")){
			String strmonth = StringUtils.join(months, ",");
			config.put("month",strmonth);
			this.processFileData();
		}else if(splitby.equals("Time")){
			String strmonth = StringUtils.join(months, ",");
			config.put("month",strmonth);
			this.processFileData();
		}
	}

	public void scpConfigFiles(Map<String, List<String>> ipfiles){

		for (String ip : ipfiles.keySet()) {
			String user = "centos";
			String pemFile = "/home/centos/mudrod/mudrod_yun.pem";
			SshOperator util = new SshOperator(ip, user, pemFile);
			List<String> files = ipfiles.get(ip);
			for(int i=0; i<files.size(); i++){
				String file = files.get(i);
				if(file.contains("config")){
					String localfile = files.get(i);
					String remotefile = "/home/centos/mudrod/jar/routing/config.json";
					try {
						util.scpFile(localfile, remotefile);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		}
	}
	
	public void invokeLogProcesserRemotely() {
		// start log processer
		for (int i=0; i<vmnum; i++) {
			String ip = ips.get(i);
			String user = "centos";
			String pemFile = "/home/centos/mudrod/mudrod_yun.pem";
			SshOperator util = new SshOperator(ip, user, pemFile);
			String cmd = "cd /home/centos/mudrod/jar/routing;  nohup java -jar -Xss5m Datamining-0.0.1-SNAPSHOT-shaded.jar config.json > filename.log 2>&1 &";
			util.executeCommand(cmd);
		}
	}
	
	public void stop(){
		if(esnode != null){
			esnode.node.close();  
		}
		
		if(spark != null){
			spark.sc.stop();
		}
	}
	
	public static void main( String[] args ) throws IOException
    {
    	String configFile = "";
    	if(args.length ==0){
			configFile = "../podacclog/config.json";
		}else{
			configFile = args[0];
		}

    	MudrodToolApp tool = new MudrodToolApp();
    	tool.init(configFile);
    	tool.preprocessData();
    	tool.invokeLogProcesserRemotely();
    	tool.stop();
    	tool.destory();
    }
	
	/*public static void main( String[] args ) throws IOException
    {
    	long startTime=System.currentTimeMillis(); 
    	
    	//invoke api 
    	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MMM-dd-HH:mm:ss");    
    	Date resultdate = new Date(startTime);
    	String sDate = sdf.format(resultdate);
    	String url = "http://localhost:8080/MudrodUtil/startJob?jobname=job-" + sDate + "&stpe=splitlog";
    	System.out.println(url);
    	jobUtil.invokeURL(url);
    	
    	String configFile = "";
    	if(args.length ==0){
			configFile = "../podacclog/config.json";
		}else{
			configFile = args[0];
		}

		try {
    		ConfigGenerator gen = new ConfigGenerator();
    		Map<String,String> config = gen.loadConfig(configFile);
    		SparkNode spark = new SparkNode(config);
    		ESNodeClient esnode = new ESNodeClient(config);
    		//get cluster node ips
    		List<String> ips = esnode.getNodeIPs();
    		int vmnum = ips.size();
    		System.out.println(ips);
    		
    		//split files tool
    		String splitby = config.get("splitby");
    		SplitLogBase splitlog = null;
    		if(splitby == null){
    			splitlog = new SplitLogByIP();
    		}else if(splitby.equals("IP")){
    			splitlog = new SplitLogByIP();
    		}else if(splitby.equals("Time")){
    			splitlog = new SplitLogByTime();
    		}

    		if(config.get("file").equals("1")){
    			String strmonth = config.get("month");
    			String[] months = strmonth.split(",");
    			for(int i=0; i<months.length; i++){
    				String HTTPfileName = config.get("accesslog") + "." + months[i];
    				String FTPfileName = config.get("ftplog") + "." + months[i];
    				List<String> splitfiles =splitlog.splitLogs(spark.sc,HTTPfileName, FTPfileName, vmnum);
    			}

    			//scp config file/ log file to each vm
    			Map<String, List<String>> ipfiles= gen.regenConfig(configFile,config, ips);
				for (String ip : ipfiles.keySet()) {
					String user = "centos";
					String pemFile = "/home/centos/mudrod/mudrod_yun.pem";
					SshOperator util = new SshOperator(ip, user, pemFile);
					List<String> files = ipfiles.get(ip);
					for(int i=0; i<files.size(); i++){
						String file = files.get(i);
						if(file.contains("config")){
							String localfile = files.get(i);
							String remotefile = "/home/centos/mudrod/jar/routing/config.json";
							util.scpFile(localfile, remotefile);
						}
					}
				}
				
				//start log processer
				for (String ip : ipfiles.keySet()) {
					String user = "centos";
					String pemFile = "/home/centos/mudrod/mudrod_yun.pem";
					SshOperator util = new SshOperator(ip, user, pemFile);
					String cmd = "cd /home/centos/mudrod/jar/routing;  nohup java -jar Datamining-0.0.1-SNAPSHOT-shaded.jar config.json > filename.log 2>&1 &";
	    			util.executeCommand(cmd);
				}
    		}
    		
    		esnode.node.close();  
    		//spark.sc.stop();

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

    	long endTime=System.currentTimeMillis();
		System.out.println("splitting is done!" + "Time elapsed： "+ (endTime-startTime)/1000+"s");
    }*/
}
