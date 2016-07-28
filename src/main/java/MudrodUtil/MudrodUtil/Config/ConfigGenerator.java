package MudrodUtil.MudrodUtil.Config;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;

import MudrodUtil.MudrodUtil.LogSplitter.SplitLogByIP;
import MudrodUtil.MudrodUtil.LogSplitter.SplitLogByTime;

public class ConfigGenerator {

	public ConfigGenerator() {
		// TODO Auto-generated constructor stub
	}

	public Map<String, List<String>> regenConfig(String configfile, Map<String,String> config, List<String> ips, String splitType) throws IOException{
		Map<String,List<String>> ipfiles = new HashMap<String,List<String>>();
		for(int i=0; i<ips.size(); i++){
		
			config.put("file", "1");
			config.put("nodetag", ips.get(i));
			config.put("splitlog", "true");
			config.put("nodeno",Integer.toString(i));
			
			if(splitType.equals("IP")){
				config.put("routing","true");
			}else if(splitType.equals("Time")){
				config.put("routing","true");
			}
			
			String newconfigfile = this.writeConfig(configfile, config, ips.get(i));
			List<String> files = new ArrayList<String>();
			files.add(newconfigfile);
			//files.addAll(splitfiles.get(i));
	
			ipfiles.put(ips.get(i), files);
		}
		
		System.out.println(ipfiles);
		
		return ipfiles;
	}
	
	public Map<String, List<String>> regenConfig(String configfile, Map<String,String> config, List<String> ips, Map<String, Integer> monthGroups) throws IOException{
		
		Map<Integer, List<String>> groupMonth = new HashMap<Integer, List<String>>();
		for(String IP : monthGroups.keySet()){
			Integer group = monthGroups.get(IP);
			if(groupMonth.get(group) == null){
				List<String> IPs = new ArrayList<String>();
				IPs.add(IP);
				groupMonth.put(group, IPs);
			}else{
				List<String> IPs = groupMonth.get(group);
				IPs.add(IP);
				groupMonth.put(group, IPs);
			}
		}
		
		Map<String,List<String>> ipfiles = new HashMap<String,List<String>>();
		for(int i=0; i<ips.size(); i++){
			config.put("file", "1");
			config.put("nodetag", ips.get(i));
			config.put("nodeno",Integer.toString(i));
			List<String> months = groupMonth.get(i);
			config.put("month",StringUtils.join(months, ","));
			config.put("routing","true");
			String newconfigfile = this.writeConfig(configfile, config, ips.get(i));
			List<String> files = new ArrayList<String>();
			files.add(newconfigfile);
			ipfiles.put(ips.get(i), files);
		}
		
		//System.out.println(ipfiles);
		
		return ipfiles;
	}
	
	public String writeConfig(String configfile,Map<String,String> config, String IP) throws IOException{
		String fileName = configfile + IP;
		File file = new File(fileName);
		if (file.exists()) {
			file.delete();			
		}
		file.createNewFile();

		FileWriter fw = new FileWriter(file.getAbsoluteFile());
		BufferedWriter bw = new BufferedWriter(fw);
		
		//System.out.println(config);
		
		bw.write("{\n");
		for(String key:config.keySet()){
			bw.write("	\"" + key + "\"" +": " + "\"" + config.get(key) + "\"," +"\n");
		}
		bw.write("}\n");
		
		bw.close();
		return fileName;
	}
	
	public Map<String, String> loadConfig(String file) throws IOException {
		// read config file
		Map<String, String> config = new HashMap<String, String>();
		BufferedReader br = new BufferedReader(new FileReader(file), 819200);
		int count = 0;
		try {
			String line = br.readLine();
			while (line != null) {
				if (line.equals("{") || line.equals("}")) {
					line = br.readLine();
					continue;
				} else {
					String[] parts = line.trim().split("\":");
					String filed = parts[0];
					String value = parts[1];
					filed = filed.substring(1, filed.length());
					value = value.endsWith(",") ? value.substring(0, value.length() - 1).trim() : value.trim();
					value = value.substring(1, value.length() - 1);
					config.put(filed, value);
				}

				line = br.readLine();
				count++;
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			br.close();

		}
		return config;
	}
}
