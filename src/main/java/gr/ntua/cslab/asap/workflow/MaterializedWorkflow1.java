package gr.ntua.cslab.asap.workflow;

import gr.ntua.cslab.asap.operators.Dataset;
import gr.ntua.cslab.asap.operators.Operator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class MaterializedWorkflow1 {
	private List<WorkflowNode> targets;
	public double optimalCost;
	public int count;
	
	@Override
	public String toString() {
		return targets.toString();
	}
	
	public MaterializedWorkflow1() {
		targets = new ArrayList<WorkflowNode>();
		optimalCost=0.0;
		count=0;
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

	public static void main(String[] args) {
		MaterializedWorkflow1 mw = new MaterializedWorkflow1();
		
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
		
	}
	
	
}
