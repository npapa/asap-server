package gr.ntua.cslab.asap.workflow;

import gr.ntua.cslab.asap.operators.AbstractOperator;
import gr.ntua.cslab.asap.operators.Operator;
import gr.ntua.cslab.asap.operators.Dataset;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

public class Workflow {
	public HashMap<Dataset,List<Operator>> datasetDag, reverseDatasetDag;
	public HashMap<Operator,List<Dataset>> operatorDag, reverseOperatorDag;
	public HashMap<Dataset,List<Dataset>> equivalentDatasets;
	public double optimalCost;
	
	public Workflow() {
		datasetDag = new HashMap<Dataset, List<Operator>>();
		operatorDag = new HashMap<Operator, List<Dataset>>();
		reverseDatasetDag = new HashMap<Dataset, List<Operator>>();
		reverseOperatorDag = new HashMap<Operator, List<Dataset>>();
		equivalentDatasets = new HashMap<Dataset, List<Dataset>>();
		optimalCost =0.0;
	}
	
	public void addInputEdge(Dataset d, Dataset tempDataset, Operator operator) {
		List<Operator> temp = datasetDag.get(tempDataset);
		if(temp==null){
			temp= new ArrayList<Operator>();
			datasetDag.put(tempDataset, temp);
		}
		temp.add(operator);
		
		List<Dataset> temp1 = reverseOperatorDag.get(operator);
		if(temp1==null){
			temp1= new ArrayList<Dataset>();
			reverseOperatorDag.put(operator, temp1);
		}
		temp1.add(tempDataset);
		
		temp1 = equivalentDatasets.get(d);
		if(temp1==null){
			temp1= new ArrayList<Dataset>();
			equivalentDatasets.put(d, temp1);
		}
		temp1.add(tempDataset);
	}

	public void addOutputEdge(Operator operator, Dataset tempDataset, Dataset d) {
		List<Operator> temp = reverseDatasetDag.get(tempDataset);
		if(temp==null){
			temp= new ArrayList<Operator>();
			reverseDatasetDag.put(tempDataset, temp);
		}
		temp.add(operator);
		
		List<Dataset> temp1 = operatorDag.get(operator);
		if(temp1==null){
			temp1= new ArrayList<Dataset>();
			operatorDag.put(operator, temp1);
		}
		temp1.add(tempDataset);
		
		temp1 = equivalentDatasets.get(d);
		if(temp1==null){
			temp1= new ArrayList<Dataset>();
			equivalentDatasets.put(d, temp1);
		}
		temp1.add(tempDataset);
	}
	
	@Override
	public String toString() {
		String ret ="Workflow: \n";
		for(Entry<Operator, List<Dataset>> e : operatorDag.entrySet()){
			ret+=e.getKey().opName+" in:{";
			for(Dataset d : reverseOperatorDag.get(e.getKey())){
				ret+= d.datasetName+", ";
			}
			ret+="} out:{";
			for(Dataset edge : e.getValue()){
				ret+=edge.datasetName+", ";
			}
			ret+="}\n";
		}
		ret+="Equivalence: \n";
		for(Entry<Dataset, List<Dataset>> e : equivalentDatasets.entrySet()){
			ret+=e.getKey().datasetName+" eq: {";
			for(Dataset edge : e.getValue()){
				ret+=edge.datasetName+", ";
			}
			ret+="}\n";
		}
		return ret;
	}
}
