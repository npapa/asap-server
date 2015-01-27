package gr.ntua.cslab.asap.operators;

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
	
}
