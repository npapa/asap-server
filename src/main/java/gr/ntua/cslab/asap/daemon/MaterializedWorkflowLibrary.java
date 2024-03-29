package gr.ntua.cslab.asap.daemon;

import gr.cslab.asap.rest.beans.WorkflowDictionary;
import gr.ntua.cslab.asap.workflow.AbstractWorkflow1;
import gr.ntua.cslab.asap.workflow.MaterializedWorkflow1;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import net.sourceforge.jeval.EvaluationException;

import org.apache.log4j.Logger;

public class MaterializedWorkflowLibrary {
	private static HashMap<String,MaterializedWorkflow1> materializedWorkflows;
	private static String workflowDirectory;

	public static void initialize(String directory) throws Exception{

		workflowDirectory = directory;
		materializedWorkflows = new HashMap<String, MaterializedWorkflow1>();
		File folder = new File(directory);
		File[] listOfFiles = folder.listFiles();

		for (int i = 0; i < listOfFiles.length; i++) {
			if (listOfFiles[i].isDirectory()) {
		        Logger.getLogger(OperatorLibrary.class.getName()).info("Loading operator: " + listOfFiles[i].getName());
		        MaterializedWorkflow1 w = new MaterializedWorkflow1(listOfFiles[i].getName());
		        w.readFromDir(listOfFiles[i].getPath());
		        materializedWorkflows.put(listOfFiles[i].getName(), w);
		    }
		}
	}
	
	public static WorkflowDictionary getWorkflow(String name) throws NumberFormatException, EvaluationException{
		return materializedWorkflows.get(name).toWorkflowDictionary();
	}

	public static List<String> getWorkflows() {
		return new ArrayList<String>(materializedWorkflows.keySet());
	}

	public static void add(MaterializedWorkflow1 workflow) throws Exception {
		
		materializedWorkflows.put(workflow.name, workflow);
		workflow.writeToDir(workflowDirectory);
	}

	public static MaterializedWorkflow1 get(String mw) {
		return materializedWorkflows.get(mw);
	}

}
