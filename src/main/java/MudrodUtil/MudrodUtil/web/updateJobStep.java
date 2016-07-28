package MudrodUtil.MudrodUtil.web;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.ExecutionException;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.gson.JsonObject;

import MudrodUtil.MudrodUtil.Cluster.jobUtil;

/**
 * Servlet implementation class updateJobStep
 */
@WebServlet("/updateJobStep")
public class updateJobStep extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public updateJobStep() {
        super();
        // TODO Auto-generated constructor stub
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		String jobname = request.getParameter("jobname");
		String step = request.getParameter("step");
		String lapsetime = request.getParameter("lapsetime");
		String nodeip = request.getParameter("nodeip");
		//http://localhost:8080/MudrodUtil/updateJobStep?jobname=job-2016-Jun-27-17:26:05&step=splitlog&laspetime=5

		PrintWriter out = response.getWriter();
		JsonObject ret = new JsonObject();
		if(jobname!=null)
		{
			response.setContentType("application/json");  
			response.setCharacterEncoding("UTF-8");
			jobUtil job = new jobUtil();
			String id;
			try {
				job.endJobSetp(jobname, step, nodeip,lapsetime);
				ret.addProperty("result", 0);
				out.print(ret.toString());
				out.flush();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ExecutionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}else{
			ret.addProperty("result", 1);
			ret.addProperty("msg", "Please input jobname and step");
			out.print(ret.toString());
			out.flush();
		}
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		doGet(request, response);
	}

}
