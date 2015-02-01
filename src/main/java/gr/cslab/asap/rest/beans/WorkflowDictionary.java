package gr.cslab.asap.rest.beans;

import java.util.ArrayList;
import java.util.List;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

@XmlRootElement(name = "workflow")
@XmlAccessorType(XmlAccessType.FIELD)
public class WorkflowDictionary {
	List<OperatorDictionary> operators;

	public WorkflowDictionary() {
		operators = new ArrayList<OperatorDictionary>();
	}

	public void addOperator(OperatorDictionary op){
		operators.add(op);
	}
	
	public List<OperatorDictionary> getOperators() {
		return operators;
	}

	public void setOperators(List<OperatorDictionary> operators) {
		this.operators = operators;
	}
	
}
