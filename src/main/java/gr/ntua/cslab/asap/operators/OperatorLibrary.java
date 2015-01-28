package gr.ntua.cslab.asap.operators;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class OperatorLibrary {
	private static List<Operator> operators;

	public static void initialize(String directory) throws IOException{
		operators = new ArrayList<Operator>();
		File folder = new File(directory);
		File[] listOfFiles = folder.listFiles();

		for (int i = 0; i < listOfFiles.length; i++) {
			if (listOfFiles[i].isFile()) {
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

	public static String getOperatorDescription(String id) {
		for(Operator op : operators){
			if(op.opName.equals(id))
				return op.toKeyValues();
		}
		// TODO Auto-generated method stub
		return "No description available";
	}
}
