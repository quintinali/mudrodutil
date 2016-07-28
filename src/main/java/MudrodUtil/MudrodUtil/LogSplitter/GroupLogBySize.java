package MudrodUtil.MudrodUtil.LogSplitter;

import java.io.File;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import MudrodUtil.MudrodUtil.PartitionLib.KGreedyPartitionSolver;
import MudrodUtil.MudrodUtil.PartitionLib.ThePartitionProblemSolver;

public class GroupLogBySize extends SplitLogBase {

	public GroupLogBySize() {
		// TODO Auto-generated constructor stub
	}
	
	public Map<String, Integer> groupLogs(Map<String, Double> monthsize, int num){
		
    	ThePartitionProblemSolver solution = new KGreedyPartitionSolver();
		Map<String, Integer> monthsGroups = solution.solve(monthsize, num);
    	
		return monthsGroups;
	}
	
	
	public static Map<String, Double> extractFileSize(List<String> months, String httpfile, String ftpfile) {

		//File file = new File(filepath);
		//File[] filelist = file.listFiles();
		
		Map<String, Double> monthsize = new HashMap<String, Double>();
		DecimalFormat df = new DecimalFormat("#.##");
		
		for (int i = 0; i < months.size(); i++) {
			String month = months.get(i);
			String httpfilename = httpfile + "." + month;
			String ftpfilename = ftpfile + "." + month;
			
			File http = new File(httpfilename);
			File ftp = new File(ftpfilename);

			double httpsize = http.length()/1073741824.0;
			double ftpsize = ftp.length()/1073741824.0;
			
			/*System.out.println(httpfilename);
			System.out.println(httpsize);
			System.out.println(ftpfilename);
			System.out.println(ftpsize);*/

			monthsize.put(month, Double.parseDouble(df.format(httpsize + ftpsize)));
		}
		
		return monthsize;
	}
	
	public List<String> splitLogs(String logpath, int num){
		
		logpath = "D:/java workspace/podacclog";
    	Map<String, Double> monthsize = extractFileSize(logpath);
    	
    	ThePartitionProblemSolver solution = new KGreedyPartitionSolver();
		Map<String, Integer> monthsGroups = solution.solve(monthsize, num);
    	
		return null;
	}
	
	
	public static Map<String, Double> extractFileSize(String filepath) {

		File file = new File(filepath);
		File[] filelist = file.listFiles();
		
		Map<String, Double> monthsize = new HashMap<String, Double>();
		DecimalFormat df = new DecimalFormat("#.##");
		
		for (int i = 0; i < filelist.length; i++) {
			if (filelist[i].isFile()) {
				String filename = filelist[i].getPath();
				String time_suffix = filename.substring(Math.max(filename.length() - 6, 0));
				double fileS = filelist[i].length()/1073741824.0;
				double curSize = 0.0;
				if(monthsize.containsKey(time_suffix)){
					curSize = monthsize.get(time_suffix);
				}
				monthsize.put(time_suffix, Double.parseDouble(df.format(curSize + fileS)));
			}
		}
		
		return monthsize;
	}

}
