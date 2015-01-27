package gr.ntua.cslab.asap.workflow;

import gr.ntua.cslab.asap.operators.Dataset;

import java.util.HashMap;

public class WorkflowDPTable {
	private HashMap<Dataset,HashMap<Dataset,Double>> dpTable;

	public WorkflowDPTable() {
		dpTable = new HashMap<Dataset, HashMap<Dataset,Double>>();
	}
	
	public void addRecord(Dataset abstractDataset, Dataset materializedDataset, Double cost){
		HashMap<Dataset, Double> map = dpTable.get(abstractDataset);
		if(map==null){
			map = new HashMap<Dataset, Double>();
			dpTable.put(abstractDataset, map);
		}
		map.put(materializedDataset, cost);
	}
	
	public HashMap<Dataset, Double> getCost(Dataset abstractDataset){
		HashMap<Dataset, Double> map = dpTable.get(abstractDataset);
		if(map==null){
			return null;
		}
		return map;
	}
	
}
