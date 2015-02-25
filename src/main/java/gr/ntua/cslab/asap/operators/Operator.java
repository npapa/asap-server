package gr.ntua.cslab.asap.operators;

import gr.ntua.cslab.asap.workflow.WorkflowNode;
import gr.ntua.ece.cslab.panic.core.containers.beans.InputSpacePoint;
import gr.ntua.ece.cslab.panic.core.containers.beans.OutputSpacePoint;
import gr.ntua.ece.cslab.panic.core.models.AbstractWekaModel;
import gr.ntua.ece.cslab.panic.core.models.Model;
import net.sourceforge.jeval.EvaluationException;
import net.sourceforge.jeval.Evaluator;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.logging.Logger;


public class Operator {
	public Model performanceModel;
	public SpecTree optree;
	public String opName;
	private static Logger logger = Logger.getLogger(Operator.class.getName());
	
	public Operator(String name) {
		optree = new SpecTree();
		opName = name;
	}
	
	public void configureModel() throws Exception {
		String modelClass = optree.getParameter("Optimization.model");
		performanceModel = (Model) Class.forName(modelClass).getConstructor().newInstance();

		HashMap<String, String> inputSpace = new HashMap<String, String>();
		optree.getNode("Optimization.inputSpace").toKeyValues("", inputSpace );
		performanceModel.setInputSpace(inputSpace);
		

		HashMap<String, String> outputSpace = new HashMap<String, String>();
		optree.getNode("Optimization.outputSpace").toKeyValues("", outputSpace );
		performanceModel.setOutputSpace(outputSpace);

		HashMap<String, String> conf = new HashMap<String, String>();
		optree.getNode("Optimization").toKeyValues("", conf );
		performanceModel.configureClassifier(conf);
		
		InputSpacePoint ip =  new InputSpacePoint();
        HashMap<String, Double> values = new HashMap<String, Double>();
        values.put("In0.uniqueKeys", 20000.0);
        values.put("In1.uniqueKeys", 10000000.0);
        values.put("cores", 10.0);
        ip.setValues(values);
        
		OutputSpacePoint op =  new 	OutputSpacePoint();
        values = new HashMap<String, Double>();
        values.put("execTime", 0.0);
        values.put("Out0.uniqueKeys", 0.0);
        op.setValues(values);
        System.out.println(performanceModel.getPoint(ip,op));
	}

	public void add(String key, String value) {
        //Logger.getLogger(Main.class.getName()).info("key: "+key+" value: "+value);
		optree.add(key,value);
	}
	
	@Override
	public String toString() {
		String ret = opName+": ";
		ret+= optree.toString();
		return ret;
	}
	
	public String toKeyValues(String separator) {
		String ret ="";
		ret+=optree.toKeyValues("", ret,separator);
		return ret;
	}
		
	public void readFromFile(String filename) throws Exception{
        File f = new File(filename+"/description");
		InputStream stream = new FileInputStream(f);
		readPropertiesFromFile(stream);
		stream.close();
		this.performanceModel = AbstractWekaModel.readFromFile(filename+"/model");
	}

	public void readFromFile(File file) throws Exception{
		readFromFile(file.toString());
	}

	public void readPropertiesFromFile(InputStream stream) throws IOException {
		Properties props = new Properties();
		props.load(stream);
		for(Entry<Object, Object> e : props.entrySet()){
			add((String)e.getKey(), (String)e.getValue());
		}
	}


	public void outputFor(Dataset d, int position, List<WorkflowNode> inputs) {
		//System.out.println("Generating output for pos: "+ position);
		d.datasetTree = optree.copyInputSubTree("Constraints.Output"+position);
		if(d.datasetTree == null)
			d.datasetTree = new SpecTree();
		
		int min = Integer.MAX_VALUE;
		for(WorkflowNode n :inputs){
			int temp = Integer.MAX_VALUE;
			if(!n.inputs.get(0).isOperator)
				temp= Integer.parseInt(n.inputs.get(0).dataset.getParameter("Optimization.uniqueKeys"));
			else
				 temp = Integer.parseInt(n.inputs.get(0).inputs.get(0).dataset.getParameter("Optimization.uniqueKeys"));
			if(temp<min){
				min=temp;
			}
		}
		d.datasetTree.add("Optimization.uniqueKeys", min+"");
	}
	
	public void writeToPropertiesFile(String directory) throws Exception {
        File dir = new File(directory);
        if (dir.exists()) {
        	dir.delete();
        }
        dir.mkdir();
        Properties props = new Properties();
		optree.writeToPropertiesFile("", props);
        File f = new File(directory+"/description");
        if (f.exists()) {
        	f.delete();
        }
    	f.createNewFile();
        OutputStream out = new FileOutputStream( f );
        props.store(out,"");
        out.close();
        
        performanceModel.serialize(directory+"/model");
	}
	
	
	public Double getCost(List<WorkflowNode> inputs) throws NumberFormatException, EvaluationException {

		logger.info("Compute cost Operator "+opName);
		logger.info("inputs: "+inputs);
		String value = getParameter("Optimization.execTime");
		logger.info("value "+value);
		Evaluator evaluator = new Evaluator();
		if(value.contains("$")){
			int offset=1;
			if(value.startsWith("\\$"))
				offset=0;
			String[] variables = value.split("\\$");
			List<String> vars = new ArrayList<String>();
			for (int i = 0; i < variables.length; i+=1) {
				logger.info("split "+variables[i]);
			}
			for (int i = offset; i < variables.length; i+=2) {
				vars.add(variables[i]);
			}
			logger.info("Variables: "+vars);
			
			for(String var : vars){
				String[] s = var.split("\\.");
				for (int i = 0; i < s.length; i+=1) {
					logger.info("split "+s[i]);
				}
				
				int inNum = Integer.parseInt(s[0]);
				WorkflowNode n = inputs.get(inNum);
				String val = null;
				if(n.isOperator)
					val=n.inputs.get(0).dataset.getParameter("Optimization."+s[1]);
				else
					val = n.dataset.getParameter("Optimization."+s[1]);
				if(val==null){
					val ="10.0";
				}
				logger.info("Replace: "+"$"+var+"$  "+ val);
				value=value.replace("$"+var+"$", val);
			}
			logger.info("Evaluate value "+value);

			logger.info("Cost: "+evaluator.evaluate(value));
			return Double.parseDouble(evaluator.evaluate(value));
		}
		else{
			logger.info("Cost: "+evaluator.evaluate(value));
			return Double.parseDouble(evaluator.evaluate(value));
		}
	}
	
	/*public Double getCost() {
		String value = getParameter("Optimization.execTime");
		return Double.parseDouble(value);
	}*/

	public String getParameter(String key) {
		return optree.getParameter(key);
	}





	public static void main(String[] args) throws Exception {
		Operator op = new Operator("HBase_HashJoin");
		op.add("Constraints.Input.number","2");
		op.add("Constraints.Output.number","1");
		op.add("Constraints.Input0.DataInfo.Attributes.number","2");
		op.add("Constraints.Input0.DataInfo.Attributes.Atr1.type","ByteWritable");
		op.add("Constraints.Input0.DataInfo.Attributes.Atr2.type","List<ByteWritable>");
		op.add("Constraints.Input0.Engine.DB.NoSQL.HBase.key","Atr1");
		op.add("Constraints.Input0.Engine.DB.NoSQL.HBase.value","Atr2");
		op.add("Constraints.Input0.Engine.DB.NoSQL.HBase.location","127.0.0.1");

		op.add("Constraints.Input1.DataInfo.Attributes.number","2");
		op.add("Constraints.Input1.DataInfo.Attributes.Atr1.type","ByteWritable");
		op.add("Constraints.Input1.DataInfo.Attributes.Atr2.type","List<ByteWritable>");
		op.add("Constraints.Input1.Engine.DB.NoSQL.HBase.key","Atr1");
		op.add("Constraints.Input1.Engine.DB.NoSQL.HBase.value","Atr2");
		op.add("Constraints.Input1.Engine.DB.NoSQL.HBase.location","127.0.0.1");

		op.add("Constraints.Output0.DataInfo.Attributes.number","2");
		op.add("Constraints.Output0.DataInfo.Attributes.Atr1.type","ByteWritable");
		op.add("Constraints.Output0.DataInfo.Attributes.Atr2.type","List<ByteWritable>");
		op.add("Constraints.Output0.Engine.DB.NoSQL.HBase.key","Atr1");
		op.add("Constraints.Output0.Engine.DB.NoSQL.HBase.value","Atr2");
		op.add("Constraints.Output0.Engine.DB.NoSQL.HBase.location","127.0.0.1");
		
		op.add("Constraints.OpSpecification.Algorithm.Join.JoinCondition","in1.atr1 = in2.atr2");
		op.add("Constraints.OpSpecification.Algorithm.Join.type", "HashJoin");

		op.add("Constraints.EngineSpecification.Distributed.MapReduce.masterLocation", "127.0.0.1");

		op.add("Optimization.model", "gr.ntua.ece.cslab.panic.core.models.UserFunction");
		op.add("Optimization.inputSpace.In0.uniqueKeys", "Integer");
		op.add("Optimization.inputSpace.In1.uniqueKeys", "Integer");
		op.add("Optimization.inputSpace.cores", "Integer");
		op.add("Optimization.outputSpace.execTime", "Double");
		op.add("Optimization.outputSpace.Out0.uniqueKeys", "Integer");
		op.add("Optimization.execTime", "100.0 + (In0.uniqueKeys + In1.uniqueKeys)/cores");
		op.add("Optimization.Out0.uniqueKeys", "In0.uniqueKeys + In1.uniqueKeys");
		
		op.configureModel();
		
		
		System.exit(0);
		
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
		
		System.out.println(op.toKeyValues("\n"));

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


	
}
