package gr.ntua.cslab.asap.operators;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Properties;

public class Dataset implements Comparable<Dataset> {

	public SpecTree datasetTree;
	public String datasetName;
	
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
		datasetTree = op.optree.copySubTree("Constraints.Input"+position);
		if(datasetTree == null)
			datasetTree = new SpecTree();
	}

	public boolean checkMatch(Dataset d) {
		return datasetTree.checkMatch(d.datasetTree);
	}

	public void outputFor(Operator op, int position) {
		datasetTree = op.optree.copySubTree("Constraints.Output"+position);
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
}
