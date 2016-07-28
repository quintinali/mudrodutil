package MudrodUtil.MudrodUtil.Cluster;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import MudrodUtil.MudrodUtil.Config.ConfigGenerator;
import MudrodUtil.MudrodUtil.Util.ESNodeClient;


public class jobUtil {
	String index;
	String type;
	ESNodeClient esnode;
	Map<String,String> config;
	
	public jobUtil() throws IOException {
		// TODO Auto-generated constructor stub
		String configFile = "D:/java workspace/podacclog/config.json";
		ConfigGenerator gen = new ConfigGenerator();
		config = gen.loadConfig(configFile);
		esnode = new ESNodeClient(config);
		
		this.index = "jobmonitor";
		this.type = "job";
		putJobMapping();
	}
	
	 public void putJobMapping() throws IOException{
		 
		 boolean exists = esnode.client.admin().indices().prepareExists(index).execute().actionGet().isExists();
			if(exists){
				return;
			}
			
			System.out.println("1");
			//set up dynamic setting, can be updated with close/ open function
			esnode.client.admin().indices().prepareCreate(index).setSettings(ImmutableSettings.settingsBuilder().loadFromSource(jsonBuilder()
	                .startObject()
	            .endObject().string())).execute().actionGet();
			
	    	XContentBuilder Mapping =  jsonBuilder()
					.startObject()
						.startObject("job")
							.startObject("properties")
								.startObject("jobname")
									.field("type", "string")
									.field("index", "not_analyzed")
								.endObject()
								.startObject("step")
									.field("type", "string")
									.field("index", "not_analyzed")
								.endObject()
								.startObject("nodeip")
									.field("type", "string")
									.field("index", "not_analyzed")
								.endObject()
								.startObject("lasttime")
									.field("type", "double")
								.endObject()
							.endObject()
						.endObject()
					.endObject();
	    	
	    	esnode.client.admin().indices()
			  .preparePutMapping(this.index)
	          .setType(this.type)
	          .setSource(Mapping)
	          .execute().actionGet();
	    }

	public String addJobStep(String jobname, String step, String ip) throws IOException {
		// TODO Auto-generated constructor stub
		XContentBuilder contentbuilder = jsonBuilder().startObject()
			.field("jobname", jobname)
			.field("step", step)
			.field("nodeip", ip)
			.field("lasttime", -1)
		.endObject();
		
		IndexRequest indexRequest = new IndexRequest(index, type).source(contentbuilder);
		IndexResponse response = esnode.client.index(indexRequest).actionGet();
		String jobId = response.getId();
		
		return jobId;
	}
	
	public String endJobSetp(String jobname, String step, String ip, String lapsetime) throws IOException, InterruptedException, ExecutionException {
		// TODO Auto-generated constructor stub
		JsonObject ret = new JsonObject();
				
		FilterBuilder filter_search = FilterBuilders.boolFilter()
				.must(FilterBuilders.termFilter("jobname",jobname))
				.must(FilterBuilders.termFilter("step", step))
				.must(FilterBuilders.termFilter("nodeip", ip));
		
		QueryBuilder query_search = QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), filter_search);
		SearchResponse response = esnode.client.prepareSearch(index)
				.setTypes(type)
		        .setQuery(query_search)
	            .setSize(1).execute().actionGet(); 
		
		int size  = response.getHits().getHits().length;

        for (SearchHit hit : response.getHits().getHits()) {
        	System.out.println(hit.getId());
        	UpdateRequest ur = new UpdateRequest(index, type, hit.getId()).doc(jsonBuilder()               
        			.startObject()
        			.field("lasttime", Double.parseDouble(lapsetime))
        			.endObject());
        	UpdateResponse updateResponse = esnode.client.update(ur).get();
        	
        	if(updateResponse.getVersion()> -1){
        		ret.addProperty("result", true);
        		ret.addProperty("version", updateResponse.getVersion());
        	}else{
        		ret.addProperty("result", "false");
        	}
        }
    	
    	if(step.equals("processlog")){
    		this.invokeMining(jobname, step);
    	}
    	return "";
	}
	
	public void invokeMining(String jobname, String step){
		FilterBuilder filter_search = FilterBuilders.boolFilter()
				.must(FilterBuilders.termFilter("jobname",jobname))
				.must(FilterBuilders.termFilter("step", step));
		
		QueryBuilder query_search = QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), filter_search);
		SearchResponse response = esnode.client.prepareSearch(index)
				.setTypes(type)
		        .setQuery(query_search)
	            .execute().actionGet(); 
		
		int totalsize  = response.getHits().getHits().length;
		
		filter_search = FilterBuilders.boolFilter()
				.must(FilterBuilders.termFilter("jobname",jobname))
				.must(FilterBuilders.termFilter("step", step))
				.must(FilterBuilders.rangeFilter("lasttime").from(0));
		
		query_search = QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), filter_search);
		response = esnode.client.prepareSearch(index)
				.setTypes(type)
		        .setQuery(query_search)
	            .execute().actionGet(); 
		
		int updatesize  = response.getHits().getHits().length;
		
		if(updatesize == totalsize){
				String ip = "10.0.3.60";
				String user = "centos";
				String pemFile = "/home/centos/mudrod/mudrod_yun.pem";
				SshOperator util = new SshOperator(ip, user, pemFile);
				String cmd = "cd /home/centos/mudrod/jar/routing;  nohup java -jar Datamining-0.0.1-SNAPSHOT-shaded.jar config.json > filename.log 2>&1 &";
				util.executeCommand(cmd);
		}
	}
	
	public static JsonObject invokeURL(String url){
		
		URL gwtServlet = null;
		String result = "";
	    try {
			URL obj = new URL(url);
			HttpURLConnection con = (HttpURLConnection) obj.openConnection();
			con.setRequestMethod("GET");
			int responseCode = con.getResponseCode();
			//System.out.println("\nSending 'GET' request to URL : " + url);
			//System.out.println("Response Code : " + responseCode);
			BufferedReader in = new BufferedReader(
			        new InputStreamReader(con.getInputStream()));
			String inputLine;
			StringBuffer response = new StringBuffer();

			while ((inputLine = in.readLine()) != null) {
				response.append(inputLine);
			}
			in.close();
			System.out.println(response.toString());
			result = response.toString();

	    } catch (MalformedURLException e) {
	        // TODO Auto-generated catch block
	        e.printStackTrace();
	    } catch (IOException e) {
	        // TODO Auto-generated catch block
	        e.printStackTrace();
	    }
	    
	    JsonParser parser = new JsonParser();
	    JsonObject json = (JsonObject) parser.parse(result);
	    
	    return json;
	}
	
	public static void main( String[] args ) throws IOException
    {
		String jobname = "";
		String step = "";
		String lapsetime = "";
		//http://localhost:8080/MudrodUtil/updateJobStep?jobname=job-2016-Jun-27-17:26:05&step=splitlog&laspetime=5
		jobname = "job-2016-Jun-27-16:54:11";
		step ="processlog";
		lapsetime="25";
		
		
		JsonObject ret = new JsonObject();
		jobUtil job = new jobUtil();
		String id;
		try {
			//id = job.addJobStep(jobname, step, "3");
			id = job.endJobSetp(jobname, step, "1", lapsetime);
			ret.addProperty("result", 0);
			ret.addProperty("jobid", id);
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
    }
}
