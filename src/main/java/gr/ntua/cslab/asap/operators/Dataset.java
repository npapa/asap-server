package gr.ntua.cslab.asap.operators;

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
}
