package MudrodUtil.MudrodUtil.LogSplitter;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.joda.time.Seconds;
import org.elasticsearch.common.joda.time.format.DateTimeFormatter;
import org.elasticsearch.common.joda.time.format.ISODateTimeFormat;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogram;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogram.Bucket;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;



public class CrawlerDetection {
	public static final String GoogleBot = "gsa-crawler (Enterprise; T4-JPDGU3TRCQAXZ; earthdata-sa@lists.nasa.gov,srinivasa.s.tummala@nasa.gov)";
	public static final String GoogleBot21 = "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)";
	public static final String BingBot ="Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)";
	public static final String YahooBot ="Mozilla/5.0 (compatible; Yahoo! Slurp; http://help.yahoo.com/help/us/ysearch/slurp)";
	public static final String RogerBot ="rogerbot/1.0 (http://www.moz.com/dp/rogerbot, rogerbot-crawler@moz.com)";
	public static final String YacyBot ="yacybot (/global; amd64 Windows Server 2008 R2 6.1; java 1.8.0_31; Europe/de) http://yacy.net/bot.html";	
	public static final String YandexBot ="Mozilla/5.0 (compatible; YandexBot/3.0; +http://yandex.com/bots)";
	public static final String GoogleImage ="Googlebot-Image/1.0";
	public static final String RandomBot1 ="Mozilla/5.0 (iPhone; CPU iPhone OS 6_0 like Mac OS X) AppleWebKit/536.26 (KHTML, like Gecko) Version/6.0 Mobile/10A5376e Safari/8536.25 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)";
	public static final String NoAgentBot = "-";

	public static final String PerlBot = "libwww-perl/";	
	public static final String ApacheHTTP ="Apache-HttpClient/";
	public static final String JavaHTTP ="Java/";
	public static final String cURL ="curl/";

	private String index = null;
	private String Cleanup_type = null;
	private String HTTP_type = null;
	private String FTP_type = null;

	public CrawlerDetection(){
			
	}

	public CrawlerDetection(String index, String HTTP_type, String FTP_type, String Cleanup_type){
		this.index = index;
		this.HTTP_type = HTTP_type;
		this.FTP_type = FTP_type;
		this.Cleanup_type = Cleanup_type;		
	}

	public boolean CheckKnownCrawler(String agent){
		if(agent.equals(GoogleBot)||agent.equals(GoogleBot21)||agent.equals(BingBot)||agent.equals(YahooBot)||agent.equals(RogerBot)||
				agent.equals(YacyBot)||agent.equals(YandexBot)||agent.equals(GoogleImage)||agent.equals(RandomBot1)||agent.equals(NoAgentBot)||
				agent.contains(PerlBot)||agent.contains(ApacheHTTP)||agent.contains(JavaHTTP)||agent.contains(cURL))
		{
			return true;
		}else{
			return false;
		}
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}
}
