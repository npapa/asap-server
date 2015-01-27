package gr.ntua.cslab.asap.server.daemon.rest;


import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.xml.ws.WebServiceException;

import org.apache.log4j.Logger;

@Path("/operators/")
public class Operators {

    public Logger logger = Logger.getLogger(Operators.class);
    
    
    @GET
    public String getApplications() {
        return "ok!!";
    }
    
}