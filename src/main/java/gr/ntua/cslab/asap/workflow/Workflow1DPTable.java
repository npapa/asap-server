package gr.ntua.cslab.asap.workflow;

import gr.ntua.cslab.asap.operators.Dataset;

import java.util.HashMap;
import java.util.List;

public class Workflow1DPTable {
	private HashMap<Dataset,List<WorkflowNode>> dpTable;
	private HashMap<Dataset,Double> dpCost;

	public Workflow1DPTable() {
		dpTable = new HashMap<Dataset,List<WorkflowNode>>();
		dpCost = new HashMap<Dataset,Double>();
	}
	
	public void addRecord(Dataset dataset, List<WorkflowNode> plan, Double cost){
		dpTable.put(dataset, plan);
		dpCost.put(dataset,cost);
	}
	
	public Double getCost(Dataset dataset){
		Double value = dpCost.get(dataset);
		if(value==null)
			return Double.MAX_VALUE;
		else
			return value;
	}
	
	public List<WorkflowNode> getPlan(Dataset dataset){
		return dpTable.get(dataset);
	}
}
