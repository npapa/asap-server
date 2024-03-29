package gr.ntua.cslab.asap.operators;

import gr.ntua.cslab.asap.workflow.WorkflowNode;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Properties;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

public class Dataset implements Comparable<Dataset> {

	public SpecTree datasetTree;
	public String datasetName;
	private static Logger logger = Logger.getLogger(Dataset.class.getName());
	
	public Dataset(String name) {
		datasetTree = new SpecTree();
		datasetName = name;
	}

	public void add(String key, String value) {
		datasetTree.add(key,value);
	}
	
	@Override
	public String toString() {
		String ret = datasetName+": ";
		ret+= datasetTree.toString();
		return ret;
	}

	@Override
	public int compareTo(Dataset o) {
		return datasetName.compareTo(o.datasetName);
	}

	public void inputFor(Operator op, int position) {
		datasetTree = op.optree.copyInputSubTree("Constraints.Input"+position);
		if(datasetTree == null)
			datasetTree = new SpecTree();
	}

	public boolean checkMatch(Dataset d) {
		//logger.info("Checking match: "+ this.toString()+"  -  "+d );
		return datasetTree.checkMatch(d.datasetTree);
	}


	
	public void outputFor(Operator op, int position) {
		//System.out.println("Generating output for pos: "+ position);
		datasetTree = op.optree.copyInputSubTree("Constraints.Output"+position);
		if(datasetTree == null)
			datasetTree = new SpecTree();
	}

	public void writeToPropertiesFile(String filename) throws IOException {
        Properties props = new Properties();

        datasetTree.writeToPropertiesFile("", props);
        File f = new File(filename);
        if (!f.exists()) {
        	f.createNewFile();
        }
        OutputStream out = new FileOutputStream( f );
        props.store(out,"");
        out.close();
	}
	

	public void readPropertiesFromFile(String filename) throws IOException{
        File f = new File(filename);
		InputStream stream = new FileInputStream(f);
		Properties props = new Properties();
		props.load(stream);
		for(Entry<Object, Object> e : props.entrySet()){
			add((String)e.getKey(), (String)e.getValue());
		}
		stream.close();
	}

	public void readPropertiesFromString(String properties) throws IOException {
		InputStream stream = new ByteArrayInputStream(properties.getBytes());
		readPropertiesFromFile(stream);
		stream.close();
	}
	
	public void readPropertiesFromFile(InputStream stream) throws IOException{
		Properties props = new Properties();
		props.load(stream);
		for(Entry<Object, Object> e : props.entrySet()){
			add((String)e.getKey(), (String)e.getValue());
		}
		stream.close();
	}
	public void readPropertiesFromFile(File file) throws IOException{
		InputStream stream = new FileInputStream(file);
		Properties props = new Properties();
		props.load(stream);
		for(Entry<Object, Object> e : props.entrySet()){
			add((String)e.getKey(), (String)e.getValue());
		}
		stream.close();
	}
	
	public String toKeyValues(String separator) {
		String ret ="";
		ret+=datasetTree.toKeyValues("", ret, separator);
		return ret;
	}

	public String getParameter(String key) {
		return datasetTree.getParameter(key);
	}


}
