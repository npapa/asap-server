package gr.ntua.cslab.asap.operators;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Properties;

public class AbstractOperator implements Comparable<AbstractOperator> {
	public SpecTree optree;
	public String opName;
	
	public AbstractOperator(String name) {
		optree = new SpecTree();
		opName = name;
	}

	public void add(String key, String value) {
		optree.add(key,value);
	}
	


	public void moveOperator(Dataset from, Dataset to) {
		optree.addAll(from.datasetTree.copyInputToOpSubTree("Constraints","Input0"));
		optree.addAll(to.datasetTree.copyInputToOpSubTree("Constraints","Output0"));
	}
	

	public void setOutput(SpecTree t, int i) {
	}

	public void setInput(SpecTree t, int i) {
		// TODO Auto-generated method stub
		
	}

	public String toKeyValues() {
		String ret ="";
		ret+=optree.toKeyValues("", ret);
		return ret;
	}
	
	@Override
	public String toString() {
		String ret = opName+": ";
		ret+= optree.toString();
		return ret;
	}

	public boolean checkMatch(Operator op) {
		return optree.checkMatch(op.optree);
	}

	public void addRegex(NodeName key, String value) {
		optree.addRegex(key,value);
	}

	@Override
	public int compareTo(AbstractOperator o) {
		return opName.compareTo(o.opName);
	}

	public void writeToPropertiesFile(String filename) throws IOException {
        Properties props = new Properties();

		optree.writeToPropertiesFile("", props);
        File f = new File(filename);
        if (!f.exists()) {
        	f.createNewFile();
        }
        OutputStream out = new FileOutputStream( f );
        props.store(out,"");
        out.close();
	}
	
}
