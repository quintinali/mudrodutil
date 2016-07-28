package  MudrodUtil.MudrodUtil.LogSplitter;


import java.io.*;
import java.lang.String;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import org.elasticsearch.action.index.IndexRequest;

/**
 * This class represents an Apache access log line. See
 * http://httpd.apache.org/docs/2.2/logs.html for more details.
 */
public class ApacheAccessLog extends WebLog implements Serializable {
	
	double Bytes;
	String Referer;
	String Browser;

	public double getBytes(){
		return this.Bytes;
	}
	
	public String getBrowser(){
		return this.Browser;
	}
	
	public ApacheAccessLog(){
		
	}

	public static WebLog parseFromLogLine(String log) throws IOException, ParseException{
		
		 String logEntryPattern = "^([\\d.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\d+|-) \"((?:[^\"]|\")+)\" \"([^\"]+)\"";
		 final int NUM_FIELDS = 9;
		 Pattern p = Pattern.compile(logEntryPattern);
		 Matcher matcher;
		
			String lineJson = "{}";
			matcher = p.matcher(log);
			if (!matcher.matches() || 
					NUM_FIELDS != matcher.groupCount()) {
				return null;
			}
			
			String time = matcher.group(4);
			time = SwithtoNum(time);
			SimpleDateFormat formatter = new SimpleDateFormat("dd/MM/yyyy");
			Date date = formatter.parse(time);

			String bytes = matcher.group(7);

			if(bytes.equals("-")){
				bytes="0";
			}

			String request = matcher.group(5).toLowerCase();
			String agent = matcher.group(9);
			CrawlerDetection crawlerDe = new CrawlerDetection();
			if(crawlerDe.CheckKnownCrawler(agent))
			{
				return null;
			}
			else
			{
				if(request.contains(".js")||request.contains(".css")||request.contains(".jpg")||request.contains(".png")||request.contains(".ico")||
						request.contains("image_captcha")||request.contains("autocomplete")||request.contains(".gif")||
						request.contains("/alldata/")||request.contains("/api/")||request.equals("get / http/1.1")||
						request.contains(".jpeg")||request.contains("/ws/"))   //request.contains("/ws/")  need to be discussed
				{
					
				}else{
					ApacheAccessLog accesslog = new ApacheAccessLog();
					accesslog.LogType = "http";
					accesslog.IP = matcher.group(1);
					accesslog.Time = time;
					accesslog._source = log;
					accesslog.day = date.getDate()/*Integer.toString(date.getDate())*/;
				
					return accesslog;
				}
			}
			return null;		
	}

}
