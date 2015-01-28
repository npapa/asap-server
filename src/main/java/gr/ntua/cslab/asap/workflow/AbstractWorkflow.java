package gr.ntua.cslab.asap.workflow;

import gr.ntua.cslab.asap.operators.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

public class AbstractWorkflow {
	public HashMap<Dataset,List<AbstractOperator>> datasetDag, reverseDatasetDag;
	public HashMap<AbstractOperator,List<Dataset>> operatorDag, reverseOperatorDag;
	public List<Dataset> materializedDatasets;
	private MaterializedOperators library;
	private int count;
	
	public AbstractWorkflow(MaterializedOperators library) {
		this.library = library;
		datasetDag = new HashMap<Dataset, List<AbstractOperator>>();
		operatorDag = new HashMap<AbstractOperator, List<Dataset>>();
		reverseDatasetDag = new HashMap<Dataset, List<AbstractOperator>>();
		reverseOperatorDag = new HashMap<AbstractOperator, List<Dataset>>();
		materializedDatasets = new ArrayList<Dataset>();
	}


	public void addInputEdge(Dataset d, AbstractOperator abstractOperator, int inputPositon) {
		List<AbstractOperator> temp = datasetDag.get(d);
		if(temp==null){
			temp= new ArrayList<AbstractOperator>();
			datasetDag.put(d, temp);
		}
		temp.add(abstractOperator);
		
		List<Dataset> temp1 = reverseOperatorDag.get(abstractOperator);
		if(temp1==null){
			temp1= new ArrayList<Dataset>();
			reverseOperatorDag.put(abstractOperator, temp1);
		}
		temp1.add(inputPositon, d);
	}

	public void addOutputEdge(AbstractOperator abstractOperator, Dataset d, int outputPosition) {
		List<AbstractOperator> temp = reverseDatasetDag.get(d);
		if(temp==null){
			temp= new ArrayList<AbstractOperator>();
			reverseDatasetDag.put(d, temp);
		}
		temp.add(abstractOperator);
		
		List<Dataset> temp1 = operatorDag.get(abstractOperator);
		if(temp1==null){
			temp1= new ArrayList<Dataset>();
			operatorDag.put(abstractOperator, temp1);
		}
		temp1.add(outputPosition, d);
	}
	
	public void addMaterializedDatasets(List<Dataset> materializedDatasets) {
		this.materializedDatasets.addAll(materializedDatasets);		
	}
	
	@Override
	public String toString() {
		String ret ="Abstract Workflow: \n";
		for(Entry<AbstractOperator, List<Dataset>> e : operatorDag.entrySet()){
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
		return ret;
	}
	

	public Workflow getWorkflow(Dataset target) {
		Workflow ret = new Workflow();
		int count =0;
		for(AbstractOperator a : operatorDag.keySet()){
			System.out.println("check matches for: "+ a.opName);
			List<Operator> operators = library.getMatches(a);
			int i = 0;
			for(Dataset d : reverseOperatorDag.get(a)){
				System.out.println("check input: "+d.datasetName);
				for(Operator op : operators){
					System.out.println(op.opName);
					Dataset tempInput = new Dataset("t"+count);
					tempInput.inputFor(op,i);
					System.out.println("Temp Input "+tempInput);
					ret.addInputEdge(d, tempInput, op);
					count++;
				}
				i++;
			}
			for(Dataset d : operatorDag.get(a)){
				for(Operator op : operators){
					ret.addOutputEdge(op, new Dataset("t"+count), d);
					count++;
				}
				
			}
		}
		
		return ret;
	}
	
	public void optimize(Dataset target, WorkflowDPTable dpTable) {
		if(dpTable.getCost(target)!=null){
			return;
		}
		if(materializedDatasets.contains(target)){
			dpTable.addRecord(target, target, new Double(0));
			return;
			//ret.put(target, new Double(0));
		}
		else{
			System.out.println("optimize: "+target.datasetName);
			List<AbstractOperator> l = reverseDatasetDag.get(target);
			int i = 0;
			for(AbstractOperator a : l){
				//find optimal materialized operator
				List<Operator> operators = library.getMatches(a);
				int jj =0;
				for(Operator op : operators){
					System.out.println("Operator: "+op.opName);
					Double maxCost = 0.0;
					int j = 0;
					boolean found0 = false;
					for(Dataset d : reverseOperatorDag.get(a)){
						optimize(d,dpTable);
						Double min = Double.MAX_VALUE;
						Dataset tempInput = new Dataset("t"+count);
						count++;
						tempInput.inputFor(op,j);
						j++;
						//System.out.println("Dataset: "+d.datasetName);
						boolean found=false;
						for(Entry<Dataset, Double> e : dpTable.getCost(d).entrySet()){
							System.out.println("Checking: \n"+tempInput);
							System.out.println(e.getKey());
							if(tempInput.checkMatch(e.getKey())){
								System.out.println("true");
								found=true;
								if(e.getValue()<min){
									min=e.getValue();
								}
							}
						}
						if(!found){
							//no much for this input
							found0=true;
							maxCost=Double.MAX_VALUE;
							break;
						}
						if(min>maxCost){
							maxCost=min;
						}
							
					}
					if(found0){
						//at least one input not matching
						maxCost=Double.MAX_VALUE;
					}
					else{
						maxCost+=op.getCost();
						Dataset tempOutput = new Dataset("t"+count);
						count++;
						tempOutput.outputFor(op,jj);
						jj++;
						dpTable.addRecord(target, tempOutput, maxCost);
						//System.out.println(maxCost);
					}
				}
				
				/*if(i>0)
					System.out.print(",");
				System.out.print(a.opName+"(");
				int ii = 0;
				for(Dataset d : reverseOperatorDag.get(a)){
					if(ii>0)
						System.out.print(",");
					optimize(d);
					ii++;
				}
				System.out.print(")");
				i++;*/
			}
		}
		
	}
	
	public Workflow optimizeWorkflow(Dataset target) {
		count =0;
		WorkflowDPTable dpTable = new WorkflowDPTable();
		optimize(target,dpTable);
		Double minCost= Double.MAX_VALUE;
		for( Entry<Dataset, Double> e : dpTable.getCost(target).entrySet() ){
			if(e.getValue()<minCost){
				minCost = e.getValue();
			}
		}
		System.out.println("minCost: "+minCost);
		/*Workflow ret = new Workflow();
		Double optCost = Double.MAX_VALUE;
		Dataset optDataset =null;
		for(Entry<Dataset, Double> e : m.entrySet()){
			if(optCost>e.getValue()){
				optCost = e.getValue();
				optDataset = e.getKey();
			}
		}
		ret.optimalCost=optCost;
		System.out.println(optDataset.datasetName+" cost: "+optCost);*/
		return null;
	}
	
	public static void main(String[] args) {
		MaterializedOperators library =  new MaterializedOperators();
		AbstractWorkflow abstractWorkflow = new AbstractWorkflow(library);
		Dataset d1 = new Dataset("hbaseDataset");
		d1.add("Constraints.DataInfo.Attributes.number","2");
		d1.add("Constraints.DataInfo.Attributes.Atr1.type","ByteWritable");
		d1.add("Constraints.DataInfo.Attributes.Atr2.type","List<ByteWritable>");
		d1.add("Constraints.Engine.DB.NoSQL.HBase.key","Atr1");
		d1.add("Constraints.Engine.DB.NoSQL.HBase.value","Atr2");
		d1.add("Constraints.Engine.DB.NoSQL.HBase.location","127.0.0.1");
		d1.add("Optimization.size","1TB");
		d1.add("Optimization.uniqueKeys","1.3 billion");

		Dataset d2 = new Dataset("mySQLDataset");
		d2.add("Constraints.DataInfo.Attributes.number","2");
		d2.add("Constraints.DataInfo.Attributes.Atr1.type","Varchar");
		d2.add("Constraints.DataInfo.Attributes.Atr2.type","Varchar");
		d2.add("Constraints.Engine.DB.Relational.MySQL.schema","...");
		d2.add("Constraints.Engine.DB.Relational.MySQL.location","127.0.0.1");
		d2.add("Optimization.size","1GB");
		d2.add("Optimization.uniqueKeys","1 million");

		AbstractOperator abstractOp = new AbstractOperator("JoinOp");
		abstractOp.add("Constraints.Input.number","2");
		abstractOp.add("Constraints.Output.number","1");
		abstractOp.add("Constraints.Input0.DataInfo.Attributes.number","2");
		abstractOp.add("Constraints.Input1.DataInfo.Attributes.number","2");
		abstractOp.add("Constraints.Output0.DataInfo.Attributes.number","2");
		abstractOp.addRegex(new NodeName("Constraints.OpSpecification.Algorithm.Join", new NodeName(".*", null, true), false), ".*");
		
		AbstractOperator abstractOp1 = new AbstractOperator("SortOp");
		abstractOp1.add("Constraints.Input.number","1");
		abstractOp1.add("Constraints.Output.number","1");
		abstractOp1.add("Constraints.Input0.DataInfo.Attributes.number","2");
		abstractOp1.add("Constraints.Output0.DataInfo.Attributes.number","2");
		abstractOp1.addRegex(new NodeName("Constraints.OpSpecification.Algorithm.Sort", new NodeName(".*", null, true), false), ".*");
		
		Dataset d3 = new Dataset("d3");
		Dataset d4 = new Dataset("d4");
		abstractWorkflow.addInputEdge(d1,abstractOp,0);
		abstractWorkflow.addInputEdge(d2,abstractOp,1);
		abstractWorkflow.addOutputEdge(abstractOp,d3,0);
		abstractWorkflow.addInputEdge(d3,abstractOp1,0);
		abstractWorkflow.addOutputEdge(abstractOp1,d4,0);
		List<Dataset> materializedDatasets = new ArrayList<Dataset>();
		materializedDatasets.add(d1);
		materializedDatasets.add(d2);
		abstractWorkflow.addMaterializedDatasets(materializedDatasets);
		System.out.println(abstractWorkflow);
		
		
		
		//Workflow workflow = abstractWorkflow.getWorkflow(library, d4);
		
		//System.out.print(d4.datasetName+"=");
		Workflow workflow1 = abstractWorkflow.optimizeWorkflow(d4);
		//System.out.println();
		System.out.println(workflow1);
	}








}
