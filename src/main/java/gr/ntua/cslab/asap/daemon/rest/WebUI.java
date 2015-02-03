package gr.ntua.cslab.asap.daemon.rest;

import gr.ntua.cslab.asap.daemon.Main;
import gr.ntua.cslab.asap.operators.OperatorLibrary;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.log4j.Logger;


@Path("/web/")
public class WebUI {

    public Logger logger = Logger.getLogger(WebUI.class);
    private static String header=readFile("header.html");
    private static String footer=readFile("footer.html");
    private static String header2=readFile("header2.html");
    private static String workflow=readFile("workflow.html");
    
    @GET
    @Produces(MediaType.TEXT_HTML)
    @Path("/main/")
    public String mainPage() throws IOException {
    	String ret = header;
    	ret+="<img src=\"../main.png\" style=\"width:100%\">\n";
    	ret += footer;
        return ret;
    }
 
    @GET
    @Produces(MediaType.TEXT_HTML)
    @Path("/operators/")
    public String listOperators() throws IOException {
    	String ret = header;
    	List<String> l = OperatorLibrary.getOperators();
    	ret += "<ul>";
    	for(String op : l){
			ret+= "<li><a href=\"operators/"+op+"\">"+op+"</a></li>";
    		
    	}
    	ret+="</ul>";
    	
    	ret += footer;
        return ret;
    }
    

    @GET
    @Produces(MediaType.TEXT_HTML)
    @Path("/operators/{id}/")
    public String operatorDescription(@PathParam("id") String id) throws IOException {
    	String ret = header2;
    	ret += "<p>"+OperatorLibrary.getOperatorDescription(id)+"</p>";
    	ret += footer;
        return ret;
    }
    

    
    @GET
    @Produces(MediaType.TEXT_HTML)
    @Path("/workflows/")
    public String listWorkflows() throws IOException {
    	String ret = header;
    	ret += "<ul>";
		ret+= "<li><a href=\"workflows/"+"DBanalytics"+"\">"+"DBanalytics"+"</a></li>";
    		
    	ret+="</ul>";
    	
    	ret += footer;
        return ret;
    }


    @GET
    @Produces(MediaType.TEXT_HTML)
    @Path("/workflows/{id}/")
    public String workflowDescription(@PathParam("id") String id) throws IOException {
    	String ret = header2+workflow;
    	
    	
    	ret += footer;
        return ret;
    }
    
    private static String readFile(String name){
    	InputStream stream = Main.class.getClassLoader().getResourceAsStream(name);
        if (stream == null) {
            System.err.println("No "+name+" file was found! Exiting...");
            System.exit(1);
        }
        BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
        StringBuilder out = new StringBuilder();
        String newLine = System.getProperty("line.separator");
        String line;
        try {
			while ((line = reader.readLine()) != null) {
			    out.append(line);
			    out.append(newLine);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
	        try {
				reader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
        return out.toString();
    }
    
}
