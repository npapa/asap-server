package gr.cslab.asap.rest.beans;

import java.util.ArrayList;
import java.util.List;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

@XmlRootElement(name = "operator")
@XmlAccessorType(XmlAccessType.FIELD)
public class OperatorDictionary {
	private String name, cost, status;
	private List<String> input;
	
	public OperatorDictionary() {
		input = new ArrayList<String>();
	}
	
	public OperatorDictionary(String name, String cost, String status) {
		this.name = name;
		this.cost = cost;
		this.status = status;
		input = new ArrayList<String>();
	}
	
	public void addInput(String in){
		input.add(in);
	}
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getCost() {
		return cost;
	}
	public void setCost(String cost) {
		this.cost = cost;
	}
	public List<String> getInput() {
		return input;
	}
	public void setInput(List<String> input) {
		this.input = input;
	}
	
	
}
