package MudrodUtil.MudrodUtil.LogSplitter;

import java.io.*;
import java.lang.String;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * This class represents an Apache access log line. See
 * http://httpd.apache.org/docs/2.2/logs.html for more details.
 */
public class FtpLog extends WebLog implements Serializable {
	public static WebLog parseFromLogLine(String log) throws IOException, ParseException {
		String orilog = log;
		String ip = log.split(" +")[6];

		//String time = log.split(" +")[1] + ":"+log.split(" +")[2] +":"+log.split(" +")[3]+":"+log.split(" +")[4];
		String day = log.split(" +")[2];
		/*time = SwithtoNum(time);
		SimpleDateFormat formatter = new SimpleDateFormat("MM:dd:HH:mm:ss:yyyy");
		Date date = formatter.parse(time);
		String bytes = log.split(" +")[7];*/
		
		String request = log.split(" +")[8].toLowerCase();
		if (!request.contains("/misc/") && !request.contains("readme")) {
			FtpLog ftplog = new FtpLog();
			ftplog.LogType = "ftp";
			ftplog.IP = ip;
			//ftplog.Time = time;
			//ftplog.Request = request;
			//ftplog.Bytes = Double.parseDouble(bytes);
			ftplog.day = Integer.parseInt(day);
			ftplog._source = orilog;
			return ftplog;
		}
		
		return null;
	}
}
