package gr.ntua.cslab.asap.daemon;

import gr.ntua.cslab.asap.operators.AbstractOperator;
import gr.ntua.cslab.asap.operators.Dataset;
import gr.ntua.cslab.asap.operators.Operator;
import gr.ntua.ece.cslab.panic.core.containers.beans.OutputSpacePoint;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;

public class OperatorLibrary {
	private static HashMap<String,Operator> operators;
	private static String operatorDirectory;
	private static Logger logger = Logger.getLogger(OperatorLibrary.class.getName());
	
	public static void initialize(String directory) throws Exception{
		operatorDirectory = directory;
		operators = new HashMap<String,Operator>();
		File folder = new File(directory);
		File[] listOfFiles = folder.listFiles();

		for (int i = 0; i < listOfFiles.length; i++) {
			if (listOfFiles[i].isDirectory()) {
		        Logger.getLogger(OperatorLibrary.class.getName()).info("Loading operator: " + listOfFiles[i].getName());
				Operator temp = new Operator(listOfFiles[i].getName());
				temp.readFromFile(listOfFiles[i]);
				operators.put(temp.opName, temp);
		    }
		}
	}
	
	public static void refresh(){
		
	}
	
	public static List<String> getOperators(){
		List<String> ret = new ArrayList<String>();
		for(Operator op : operators.values()){
			ret.add(op.opName);
		}
		return ret;
	}
	
	public static List<Operator> getMatches(AbstractOperator abstractOperator){
		//logger.info("Check matches: "+abstractOperator.opName);
		List<Operator> ret = new ArrayList<Operator>();
		for(Operator op : operators.values()){
			if(abstractOperator.checkMatch(op))
				ret.add(op);
		}
		//for(Operator o :ret){
		//	logger.info("Found: "+o.opName);
		//}
		return ret;
	}
	
	public static List<Operator> checkMove(Dataset from, Dataset to) {
		//logger.info("Check move from: "+from+" to: "+to);
		AbstractOperator abstractMove = new AbstractOperator("move");
		abstractMove.moveOperator(from,to);
		return getMatches(abstractMove);
	}

	public static String getOperatorDescription(String id) {
		Operator op = operators.get(id);
		if(op==null)
			return "No description available";
		return op.toKeyValues("\n");
	}

	public static void add(Operator o) {
		operators.put(o.opName, o);
	}

	public static void addOperator(String opname, String opString) throws Exception {
    	Operator o = new Operator(opname);
    	InputStream is = new ByteArrayInputStream(opString.getBytes());
    	o.readPropertiesFromFile(is);
    	o.writeToPropertiesFile("asapLibrary/operators/"+o.opName);
    	add(o);
    	is.close();
	}

	public static void deleteOperator(String opname) {
		Operator op = operators.remove(opname);
		File file = new File(operatorDirectory+"/"+op.opName);
		file.delete();
	}
	

	public static Operator getOperator(String opname) {
		return operators.get(opname);
	}

	public static String getProfile(String opname) {
		Operator op = operators.get(opname);
		List<OutputSpacePoint> values = op.performanceModel.getOriginalPointValues();
		System.out.println(values.toString());
		if(opname.equals("Sort"))
			return "/terasort.csv";
		else
			return "/iris.csv";
			
	}

}
