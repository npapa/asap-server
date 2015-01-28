package gr.ntua.cslab.asap.operators;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.Properties;

public class Operator {
	public SpecTree optree;
	public String opName;
	
	public Operator(String name) {
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
	
	public String toKeyValues() {
		String ret ="";
		ret+=optree.toKeyValues("", ret);
		return ret;
	}
	
	public void writeToPropertiesFile(String filename) {
		try {
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
	    catch (Exception e ) {
	        e.printStackTrace();
	    }
		
	}

	public static void main(String[] args) {
		Operator op = new Operator("HBase_HashJoin");
		op.add("Constraints.Input1.DataInfo.Attributes.number","2");
		op.add("Constraints.Input1.DataInfo.Attributes.Atr1.type","ByteWritable");
		op.add("Constraints.Input1.DataInfo.Attributes.Atr2.type","List<ByteWritable>");
		op.add("Constraints.Input1.Engine.DB.NoSQL.HBase.key","Atr1");
		op.add("Constraints.Input1.Engine.DB.NoSQL.HBase.value","Atr2");
		op.add("Constraints.Input1.Engine.DB.NoSQL.HBase.location","127.0.0.1");

		op.add("Constraints.Input2.DataInfo.Attributes.number","2");
		op.add("Constraints.Input2.DataInfo.Attributes.Atr1.type","ByteWritable");
		op.add("Constraints.Input2.DataInfo.Attributes.Atr2.type","List<ByteWritable>");
		op.add("Constraints.Input2.Engine.DB.NoSQL.HBase.key","Atr1");
		op.add("Constraints.Input2.Engine.DB.NoSQL.HBase.value","Atr2");
		op.add("Constraints.Input2.Engine.DB.NoSQL.HBase.location","127.0.0.1");

		op.add("Constraints.Output1.DataInfo.Attributes.number","2");
		op.add("Constraints.Output1.DataInfo.Attributes.Atr1.type","ByteWritable");
		op.add("Constraints.Output1.DataInfo.Attributes.Atr2.type","List<ByteWritable>");
		op.add("Constraints.Output1.Engine.DB.NoSQL.HBase.key","Atr1");
		op.add("Constraints.Output1.Engine.DB.NoSQL.HBase.value","Atr2");
		op.add("Constraints.Output1.Engine.DB.NoSQL.HBase.location","127.0.0.1");
		
		op.add("Constraints.OpSpecification.Algorithm.Join.JoinCondition","in1.atr1 = in2.atr2");
		op.add("Constraints.OpSpecification.Algorithm.Join.type", "HashJoin");

		//op.add("Properties.MaintainTags", ".*");
		
		System.out.println(op.toKeyValues());

		Operator op1 = new Operator("HBase_HashJoin");
		op1.add("Constraints.Input1.DataInfo.Attributes.number","1");
		op1.add("Constraints.Input1.DataInfo.Attributes.Atr1.type","ByteWritable");
		op1.add("Constraints.Input1.DataInfo.Attributes.Atr2.type","List<ByteWritable>");
		op1.add("Constraints.Input1.Engine.DB.NoSQL.HBase.key","Atr1");
		op1.add("Constraints.Input1.Engine.DB.NoSQL.HBase.value","Atr2");
		op1.add("Constraints.Input1.Engine.DB.NoSQL.HBase.location","127.0.0.1");

		op1.add("Constraints.Input2.DataInfo.Attributes.number","2");
		op1.add("Constraints.Input2.DataInfo.Attributes.Atr1.type","ByteWritable");
		op1.add("Constraints.Input2.DataInfo.Attributes.Atr2.type","List<ByteWritable>");
		op1.add("Constraints.Input2.Engine.DB.NoSQL.HBase.key","Atr1");
		op1.add("Constraints.Input2.Engine.DB.NoSQL.HBase.value","Atr2");
		op1.add("Constraints.Input2.Engine.DB.NoSQL.HBase.location","127.0.0.1");

		op1.add("Constraints.Output1.DataInfo.Attributes.number","2");
		op1.add("Constraints.Output1.DataInfo.Attributes.Atr1.type","ByteWritable");
		op1.add("Constraints.Output1.DataInfo.Attributes.Atr2.type","List<ByteWritable>");
		op1.add("Constraints.Output1.Engine.DB.NoSQL.HBase.key","Atr1");
		op1.add("Constraints.Output1.Engine.DB.NoSQL.HBase.value","Atr2");
		op1.add("Constraints.Output1.Engine.DB.NoSQL.HBase.location","127.0.0.1");
		
		op1.add("Constraints.OpSpecification.Algorithm.Join.JoinCondition","in1.atr1 = in2.atr2");
		op1.add("Constraints.OpSpecification.Algorithm.Join.type", "MergeJoin");
		
		op1.writeToPropertiesFile("/home/nikos/test1");

		System.exit(0);
		AbstractOperator abstractOp = new AbstractOperator("JoinOp");
		abstractOp.add("Constraints.Input1.DataInfo.Attributes.number","1");
		abstractOp.add("Constraints.Input2.DataInfo.Attributes.number","2");
		abstractOp.add("Constraints.Output1.DataInfo.Attributes.number","2");
		abstractOp.addRegex(new NodeName("Constraints.OpSpecification.Algorithm.Join", new NodeName(".*", null, true), false), ".*");
		//abstractOp.add("Constraints.OpSpecification.Algorithm.Join.type","(HashJoin|MergeJoin)");

		System.out.println(abstractOp);
		long start = System.currentTimeMillis();
		/*for (int i = 0; i < 1000; i++) {
			System.out.println(abstractOp.checkMatch(op));
			System.out.println(abstractOp.checkMatch(op1));
		}*/
		System.out.println(abstractOp.checkMatch(op));
		System.out.println(abstractOp.checkMatch(op1));
		long stop = System.currentTimeMillis();
		System.out.println("Time (s): "+((double)(stop-start))/1000.0);
	}

	public Double getCost() {
		String value = getParameter("Optimization.execTime");
		return Double.parseDouble(value);
	}

	public String getParameter(String key) {
		return optree.getParameter(key);
	}

}
