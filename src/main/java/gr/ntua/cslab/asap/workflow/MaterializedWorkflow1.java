package gr.ntua.cslab.asap.workflow;

import gr.cslab.asap.rest.beans.WorkflowDictionary;
import gr.ntua.cslab.asap.operators.Dataset;
import gr.ntua.cslab.asap.operators.Operator;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;

import net.sourceforge.jeval.EvaluationException;

import org.apache.log4j.Logger;

public class MaterializedWorkflow1 {
	private List<WorkflowNode> targets;
	private HashMap<String,List<WorkflowNode>> bestPlans;
	public double optimalCost;
	public int count;
	public String name;
	private static Logger logger = Logger.getLogger(MaterializedWorkflow1.class.getName());
	
	@Override
	public String toString() {
		return targets.toString();
	}
	
	public MaterializedWorkflow1(String name) {
		this.name = name;
		targets = new ArrayList<WorkflowNode>();
		bestPlans = new HashMap<String, List<WorkflowNode>>();
		optimalCost=0.0;
		count=0;
	}

	public void setBestPlan(String target, List<WorkflowNode> plan){
		logger.info("Target: "+target);
		logger.info("Best plan: ");
		for(WorkflowNode w : plan)
			logger.info(w.toStringNorecursive());
		bestPlans.put(target, plan);
	}
	
	public void setAllNotVisited(){

		for(WorkflowNode t : targets){
			t.setAllNotVisited();
		}
	}
	
	public void printNodes() {
		for(WorkflowNode t : targets){
			t.setAllNotVisited();
		}
		for(WorkflowNode t : targets){
			t.printNodes();
		}
	}
	
	public WorkflowDictionary toWorkflowDictionary() throws NumberFormatException, EvaluationException {
		for(WorkflowNode t : targets){
			t.setAllNotVisited();
		}
		WorkflowDictionary ret = new WorkflowDictionary();
    	for(WorkflowNode target : targets){
    		target.toWorkflowDictionary(ret, bestPlans);
    	}
		return ret;
	}
	

	public void writeToDir(String directory) throws IOException {
		directory+="/"+name;
		for(WorkflowNode t : targets){
			t.setAllNotVisited();
		}
		
        File workflowDir = new File(directory);
        if (!workflowDir.exists()) {
        	workflowDir.mkdirs();
        }
        File opDir = new File(directory+"/operators");
        if (!opDir.exists()) {
        	opDir.mkdirs();
        }
        File datasetDir = new File(directory+"/datasets");
        if (!datasetDir.exists()) {
        	datasetDir.mkdirs();
        }
        File edgeGraph = new File(directory+"/graph");
    	FileOutputStream fos = new FileOutputStream(edgeGraph);
    	 
    	BufferedWriter graphWritter = new BufferedWriter(new OutputStreamWriter(fos));
    	

		for(WorkflowNode t : targets){
			t.writeToDir(directory+"/operators",directory+"/datasets",graphWritter);
			graphWritter.write(t.toStringNorecursive() +",$$target");
			graphWritter.newLine();
		}
    	
		graphWritter.close();
        
        
	}

	public void readFromDir(String directory) throws IOException {
		HashMap<String,WorkflowNode> nodes = new HashMap<String, WorkflowNode>();
		File folder = new File(directory+"/operators");
		File[] files = folder.listFiles();

		for (int i = 0; i < files.length; i++) {
			if (files[i].isFile()) {
				WorkflowNode n = new WorkflowNode(true, false);
				Operator temp = new Operator(files[i].getName());
				temp.readPropertiesFromFile(files[i]);
				n.setOperator(temp);
				nodes.put(temp.opName, n);
			} 
		}
		folder = new File(directory+"/datasets");
		files = folder.listFiles();

		for (int i = 0; i < files.length; i++) {
			if (files[i].isFile()) {
				WorkflowNode n = new WorkflowNode(false, false);
				Dataset temp = new Dataset(files[i].getName());
				temp.readPropertiesFromFile(files[i]);
				n.setDataset(temp);
				nodes.put(temp.datasetName, n);
			} 
		}
		
		File edgeGraph = new File(directory+"/graph");
		FileInputStream fis = new FileInputStream(edgeGraph);
		BufferedReader br = new BufferedReader(new InputStreamReader(fis));
	 
		String line = null;
		while ((line = br.readLine()) != null) {
			String[] e =line.split(",");
			if(e[1].equals("$$target")){
				this.targets.add(nodes.get(e[0]));
			}
			else{
				WorkflowNode src = nodes.get(e[0]);
				WorkflowNode dest = nodes.get(e[1]);
				dest.inputs.add(src);
			}
		}
		br.close();
	}
	
	public void addTarget(WorkflowNode target) {
		targets.add(target);
	}

	public void addTargets(List<WorkflowNode> targets) {
		this.targets.addAll(targets);
	}
	
	public List<WorkflowNode> getTargets() {
		return targets;
	}
	
	

	public static void main(String[] args) throws IOException {
		/*MaterializedWorkflow1 mw = new MaterializedWorkflow1();
		
		WorkflowNode t = new WorkflowNode(false,false);
		t.setDataset(new Dataset("out2"));
		mw.addTarget(t);

		WorkflowNode t1 = new WorkflowNode(true,false);
		t1.setOperator(new Operator("op2_1"));
		t.addInput(t1);
		WorkflowNode t2 = new WorkflowNode(true,false);
		t2.setOperator(new Operator("op2_2"));
		t.addInput(t2);
		

		WorkflowNode t31 = new WorkflowNode(false,false);
		t31.setDataset(new Dataset("in1.1"));
		WorkflowNode t32 = new WorkflowNode(false,false);
		t32.setDataset(new Dataset("in1.2"));
		t1.addInput(t31);
		t1.addInput(t32);


		WorkflowNode t33 = new WorkflowNode(false,false);
		t33.setDataset(new Dataset("in2.1"));
		WorkflowNode t34 = new WorkflowNode(false,false);
		t34.setDataset(new Dataset("in2.2"));
		t2.addInput(t33);
		t2.addInput(t34);
		
		System.out.println(mw);
		mw.printNodes();
		*/
		MaterializedWorkflow1 mw = new MaterializedWorkflow1("sd");
		
		mw.readFromDir("asapLibrary/workflows/latest");
		System.out.println(mw);
	}




	
	
}
