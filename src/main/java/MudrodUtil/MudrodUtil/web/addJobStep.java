package MudrodUtil.MudrodUtil.web;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.gson.JsonObject;

import MudrodUtil.MudrodUtil.Cluster.jobUtil;

/**
 * Servlet implementation class addJob
 */
@WebServlet("/startJob")
public class addJobStep extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public addJobStep() {
        super();
        // TODO Auto-generated constructor stub
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		String jobname = request.getParameter("jobname");
		String step = request.getParameter("step");
		String nodeip = request.getParameter("nodeip");
		System.out.println(step);
		PrintWriter out = response.getWriter();
		JsonObject ret = new JsonObject();
		if(jobname!=null)
		{
			response.setContentType("application/json");  
			response.setCharacterEncoding("UTF-8");
			jobUtil job = new jobUtil();
			String id = job.addJobStep(jobname,step, nodeip);
			ret.addProperty("result", 0);
			ret.addProperty("jobid", id);
			out.print(ret.toString());
			out.flush();
		}else{
			ret.addProperty("result", 1);
			ret.addProperty("msg", "Please input jobname");
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
