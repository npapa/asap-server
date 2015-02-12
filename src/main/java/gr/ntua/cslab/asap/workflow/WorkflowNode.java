package gr.ntua.cslab.asap.workflow;

import gr.cslab.asap.rest.beans.OperatorDictionary;
import gr.cslab.asap.rest.beans.WorkflowDictionary;
import gr.ntua.cslab.asap.daemon.OperatorLibrary;
import gr.ntua.cslab.asap.operators.AbstractOperator;
import gr.ntua.cslab.asap.operators.Dataset;
import gr.ntua.cslab.asap.operators.Operator;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.log4j.Logger;

public class WorkflowNode implements Comparable<WorkflowNode>{
	private boolean visited;
	public boolean isOperator,isAbstract;
	public Operator operator;
	public AbstractOperator abstractOperator;
	public Dataset dataset;
	public List<WorkflowNode> inputs;
	private static Logger logger = Logger.getLogger(WorkflowNode.class.getName());
	
	public WorkflowNode(boolean isOperator, boolean isAbstract) {
		this.isOperator = isOperator;
		this.isAbstract = isAbstract;
		inputs = new ArrayList<WorkflowNode>();
		visited=false;
	} 
	
	public void setOperator(Operator operator){
		this.operator=operator;
	}
	
	public void setAbstractOperator(AbstractOperator abstractOperator){
		this.abstractOperator=abstractOperator;
	}
	
	public void setDataset(Dataset dataset){
		this.dataset=dataset;
	}
	
	public void addInput(WorkflowNode input){
		inputs.add(input);
	}

	public void addInputs(List<WorkflowNode> inputs){
		this.inputs.addAll(inputs);
	}


	public List<WorkflowNode> materialize(MaterializedWorkflow1 materializedWorkflow) {
		//logger.info("Processing : "+toStringNorecursive());
		List<WorkflowNode> ret = new ArrayList<WorkflowNode>();
		List<List<WorkflowNode>> materializedInputs = new ArrayList<List<WorkflowNode>>();
		for(WorkflowNode in : inputs){
			List<WorkflowNode> l = in.materialize(materializedWorkflow);
			materializedInputs.add(l);
		}
		//logger.info(materializedInputs);
		if(isOperator){
			if(isAbstract){
				List<Operator> operators = OperatorLibrary.getMatches(abstractOperator);
				for(Operator op : operators){
					//logger.info("Materialized operator: "+op.opName);
					WorkflowNode temp = new WorkflowNode(true, false);
					temp.setOperator(op);
					int inputs = Integer.parseInt(op.getParameter("Constraints.Input.number"));
					boolean inputsMatch=true;
					for (int i = 0; i < inputs; i++) {
						Dataset tempInput = new Dataset("t"+materializedWorkflow.count);
						materializedWorkflow.count++;
						tempInput.inputFor(op,i);
						WorkflowNode tempInputNode = new WorkflowNode(false, false);
						tempInputNode.setDataset(tempInput);
						temp.addInput(tempInputNode);
						

						boolean inputMatches=false;
						for(WorkflowNode in : materializedInputs.get(i)){
							logger.info("Checking: "+in.dataset.datasetName);
							if(tempInput.checkMatch(in.dataset)){
								logger.info("true");
								inputMatches=true;
								tempInputNode.addInput(in);
							}
							else{
								//check move
								logger.info("Check move ");
								List<Operator> moveOps = OperatorLibrary.checkMove(in.dataset, tempInput);
								if(!moveOps.isEmpty()){
									logger.info("true");
									inputMatches=true;
									for(Operator m : moveOps){
										WorkflowNode moveNode = new WorkflowNode(true, false);
										moveNode.setOperator(m);
										moveNode.addInput(in);
										tempInputNode.addInput(moveNode);
									}
									
								}

								
							}
						}
						if(!inputMatches){
							inputsMatch=false;
							break;
						}
						//System.out.println(materializedInputs.get(i)+"fb");
						//tempInputNode.addInputs(materializedInputs.get(i));
					}
					if(inputsMatch){
						logger.info("all inputs match");
						WorkflowNode tempOutputNode = new WorkflowNode(false, false);
						Dataset tempOutput = new Dataset("t"+materializedWorkflow.count);
						materializedWorkflow.count++;
						tempOutput.outputFor(op, 0);
						tempOutputNode.setDataset(tempOutput);
						tempOutputNode.addInput(temp);
						ret.add(tempOutputNode);
					}
				}
			}
			else{
				
			}
		}
		else{
			if(isAbstract){

				/*WorkflowNode temp = new WorkflowNode(false, false);
				temp.setDataset(dataset);
				for(List<WorkflowNode> l : materializedInputs){
					temp.addInputs(l);
				}
				ret.add(temp);*/
				for(List<WorkflowNode> l : materializedInputs){
					ret.addAll(l);
				}
			}
			else{
				WorkflowNode temp = new WorkflowNode(false, false);
				temp.setDataset(dataset);
				for(List<WorkflowNode> l : materializedInputs){
					temp.addInputs(l);
				}
				ret.add(temp);
			}			
		}
		//System.out.println("Finished : "+toStringNorecursive());
		return ret;
	}
	
	
	
	@Override
	public int compareTo(WorkflowNode o) {
		if(this.isOperator != o.isOperator){
			if(this.isOperator)
				return -1;
			else
				return 1;
		}
		else{
			if(this.isOperator)
				return this.operator.opName.compareTo(o.operator.opName);
			else
				return this.dataset.compareTo(o.dataset);
		}
	}

	public String toStringNorecursive() {
		String ret = "";
		if(isOperator){
			if(isAbstract)
				ret+=abstractOperator.opName;
			else
				ret+=operator.opName;
		}
		else{
			ret+=dataset.datasetName;
		}
		return ret;
	}
	
	@Override
	public String toString() {
		String ret = "";
		if(isOperator){
			if(isAbstract)
				ret+=abstractOperator.opName;
			else
				ret+=operator.opName;
		}
		else{
			ret+=dataset.datasetName;
		}
		if(inputs.size()>0){
			ret+=" { ";
			int i=0;
			for(WorkflowNode n : inputs){
				if(i!=0)
					ret+=", ";
				ret+=n.toString();
				i++;
			}
			ret+=" }";
		}
		return ret;
	}

	public void printNodes() {
		if(!visited){
			for(WorkflowNode n : inputs){
				System.out.println(n.toStringNorecursive() +"->"+toStringNorecursive());
			}
			for(WorkflowNode n : inputs){
				n.printNodes();
			}
			visited=true;
		}
	}


	public void toWorkflowDictionary(WorkflowDictionary ret, Random ran) {
		if(!visited){
	    	OperatorDictionary op = new OperatorDictionary(toStringNorecursive(), ran.nextInt(1000)+"", "running", isOperator+"", toStringNorecursive()+"\n"+toKeyValueString());

			for(WorkflowNode n : inputs){
				op.addInput(n.toStringNorecursive());
				n.toWorkflowDictionary(ret, ran);
			}
	    	ret.addOperator(op);
			visited=true;
		}
		
	}
	
	public String toKeyValueString() {
		if(isOperator){
			if(isAbstract){
				return abstractOperator.toKeyValues("\n");
			}
			else{
				return operator.toKeyValues("\n");
			}
		}
		else{
			return dataset.toKeyValues("\n");
		}
	}

	public void writeToDir(String opDir, String datasetDir,BufferedWriter graphWritter) throws IOException {

		if(!visited){

			if(isOperator){
				if(isAbstract)
					abstractOperator.writeToPropertiesFile(opDir+"/"+abstractOperator.opName);
				else
					operator.writeToPropertiesFile(opDir+"/"+operator.opName);
			}
			else{
				dataset.writeToPropertiesFile(datasetDir+"/"+dataset.datasetName);
			}
			for(WorkflowNode n : inputs){
				graphWritter.write(n.toStringNorecursive() +","+toStringNorecursive());
				graphWritter.newLine();
			}
			for(WorkflowNode n : inputs){
				n.writeToDir(opDir, datasetDir, graphWritter);
			}
			visited=true;
		}
	}
	
	public void graphToString(BufferedWriter graphWritter) throws IOException {
		for(WorkflowNode n : inputs){
			graphWritter.write(n.toStringNorecursive() +","+toStringNorecursive());
			graphWritter.newLine();
		}
	}

	public void graphToStringRecursive(BufferedWriter graphWritter) throws IOException {

		if(!visited){
			for(WorkflowNode n : inputs){
				graphWritter.write(n.toStringNorecursive() +","+toStringNorecursive());
				graphWritter.newLine();
			}
			for(WorkflowNode n : inputs){
				n.graphToString(graphWritter);
			}
			visited=true;
		}
	}

	public void setAllNotVisited() {
		visited=false;
		for(WorkflowNode n : inputs){
			n.setAllNotVisited();
		}
	}



}
