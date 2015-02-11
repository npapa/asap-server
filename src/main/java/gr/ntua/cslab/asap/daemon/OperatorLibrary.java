package gr.ntua.cslab.asap.daemon;

import gr.ntua.cslab.asap.operators.AbstractOperator;
import gr.ntua.cslab.asap.operators.Dataset;
import gr.ntua.cslab.asap.operators.Operator;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

public class OperatorLibrary {
	private static List<Operator> operators;

	public static void initialize(String directory) throws IOException{
		operators = new ArrayList<Operator>();
		File folder = new File(directory);
		File[] listOfFiles = folder.listFiles();

		for (int i = 0; i < listOfFiles.length; i++) {
			if (listOfFiles[i].isFile() && !listOfFiles[i].isHidden()) {
		        Logger.getLogger(OperatorLibrary.class.getName()).info("Loading operator: " + listOfFiles[i].getName());
				Operator temp = new Operator(listOfFiles[i].getName());
				temp.readPropertiesFromFile(listOfFiles[i]);
				operators.add(temp);
		    }
		}
	}
	
	public static void refresh(){
		
	}
	
	public static List<String> getOperators(){
		List<String> ret = new ArrayList<String>();
		for(Operator op : operators){
			ret.add(op.opName);
		}
		return ret;
	}
	
	public static List<Operator> getMatches(AbstractOperator abstractOperator){
		List<Operator> ret = new ArrayList<Operator>();
		for(Operator op : operators){
			if(abstractOperator.checkMatch(op))
				ret.add(op);
		}
		return ret;
	}
	
	public static List<Operator> checkMove(Dataset from, Dataset to) {
		AbstractOperator abstractMove = new AbstractOperator("move");
		abstractMove.moveOperator(from,to);
		return getMatches(abstractMove);
	}

	public static String getOperatorDescription(String id) {
		for(Operator op : operators){
			if(op.opName.equals(id))
				return op.toKeyValues("<br>");
		}
		// TODO Auto-generated method stub
		return "No description available";
	}

	public static void add(Operator o) {
		operators.add(o);
	}

	public static void addOperator(String opname, String opString) throws IOException {
    	Operator o = new Operator(opname);
    	InputStream is = new ByteArrayInputStream(opString.getBytes());
    	o.readPropertiesFromFile(is);
    	o.writeToPropertiesFile("asapLibrary/operators/"+o.opName);
    	add(o);
    	is.close();
	}
}
