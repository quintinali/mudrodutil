package MudrodUtil.MudrodUtil.Cluster;
import java.io.BufferedReader;
import java.io.CharArrayWriter;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Date;

import ch.ethz.ssh2.Connection;
import ch.ethz.ssh2.SCPClient;
import ch.ethz.ssh2.Session;
import ch.ethz.ssh2.StreamGobbler;

/**
 * @author lizhenlong
 * One SshOperator instance is one connection to one machine.
 */

//find . -name "*.acc*"  

public class SshOperator {

	String hostname = "localhost";//"199.26.254.166";
	String username = "root";
	String password = "xiuweili";
	File pemFile;
	Connection conn=null;
	
	public String getHostname() {
		return hostname;
	}

	public String getUsername() {
		return username;
	}

	public Connection getConn() {
		return conn;
	}

	public SshOperator(String hostname, String username, String pemFilePath){
		this.hostname = hostname;
		this.username = username;
		this.pemFile = new File(pemFilePath);

		try {
			Connection conn = new Connection(hostname);
			conn.connect();
			boolean isAuthenticated = false;
			isAuthenticated = conn.authenticateWithPublicKey(username, pemFile, null);
			if (isAuthenticated == true) {
				this.conn = conn;
			} else {
				this.conn = null;
				throw new IOException("Authentication failed.");
			}
		} catch (IOException e) {
			e.printStackTrace(System.err);
			this.conn = null;
		}
	}
	
	public void closeConnection(){
		this.conn.close();
	}

	public ArrayList<String> executeCommand(String cmd){
		ArrayList<String> stdoutStr = new ArrayList<String>();
		try {
			Session sess = this.conn.openSession();
			sess.execCommand(cmd);
			InputStream stdout = new StreamGobbler(sess.getStdout());
			BufferedReader br = new BufferedReader(new InputStreamReader(stdout));
			while (true)
			{
				String line = br.readLine();
				if (line == null)
					break;
				System.out.println(line);
				stdoutStr.add(line);
			}
			/* Show exit status, if available (otherwise "null") */
			System.out.println("###SshOperator.executeCommand:"+cmd+" ; ExitCode: " + sess.getExitStatus());
			//System.out.println("-------------------------------------------------");
			sess.close();
		} catch (IOException e) {
			e.printStackTrace(System.err);
		}
		return stdoutStr;
	}
	
	public void scpFile(String localFile, String remoteFile) throws IOException{
		SCPClient scp = this.conn.createSCPClient();
		if (remoteFile.contains("/"))
		{
			String dir = remoteFile.substring(0, remoteFile.lastIndexOf("/"));
			String file = remoteFile.substring(remoteFile.lastIndexOf("/") + 1);
			scp.put(localFile, file, dir, "0644");
		}
		else
		{
			scp.put(localFile, remoteFile, "", "0644");
		}
		System.out.println("upload file complete" + scp.toString());
	}
}
