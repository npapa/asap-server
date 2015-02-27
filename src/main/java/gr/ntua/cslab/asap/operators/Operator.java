package gr.ntua.cslab.asap.operators;

import gr.ntua.cslab.asap.workflow.WorkflowNode;
import gr.ntua.ece.cslab.panic.core.containers.beans.InputSpacePoint;
import gr.ntua.ece.cslab.panic.core.containers.beans.OutputSpacePoint;
import gr.ntua.ece.cslab.panic.core.models.AbstractWekaModel;
import gr.ntua.ece.cslab.panic.core.models.Model;
import gr.ntua.ece.cslab.panic.core.samplers.Sampler;
import gr.ntua.ece.cslab.panic.core.samplers.UniformSampler;
import net.sourceforge.jeval.EvaluationException;
import net.sourceforge.jeval.Evaluator;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.logging.Logger;


public class Operator {
	public HashMap<String,Model> models;
	public HashMap<String, String> inputSpace, outputSpace;
	public SpecTree optree;
	public String opName;
	private static Logger logger = Logger.getLogger(Operator.class.getName());
	private String directory;
	
	public Operator(String name, String directory) {
		optree = new SpecTree();
		opName = name;
		models = new HashMap<String, Model>();
		this.directory=directory;
	}
	

	/*public void readModel(File file) throws Exception {
		String modelClass = optree.getParameter("Optimization.model");
		if(modelClass==null){
			performanceModel = AbstractWekaModel.readFromFile(file.toString()+"/model");
		}
	}*/
	
	/**
	 * @throws Exception
	 */
	public void configureModel() throws Exception {

		inputSpace = new HashMap<String, String>();
		optree.getNode("Optimization.inputSpace").toKeyValues("", inputSpace );
		outputSpace = new HashMap<String, String>();
		optree.getNode("Optimization.outputSpace").toKeyValues("", outputSpace );
		
		for(Entry<String, String> e : outputSpace.entrySet()){
			Model performanceModel=null;
			String modelClass = optree.getParameter("Optimization.model."+e.getKey());
			//System.out.println(e.getKey()+" class: "+modelClass);
			if(modelClass.contains("AbstractWekaModel"))
				performanceModel = AbstractWekaModel.readFromFile(directory+"/"+e.getKey()+".model");
			else
				performanceModel = (Model) Class.forName(modelClass).getConstructor().newInstance();
			
			performanceModel.setInputSpace(inputSpace);
			HashMap<String, String> temp = new HashMap<String, String>();
			temp.put(e.getKey(), e.getValue());
			performanceModel.setOutputSpace(temp);

			HashMap<String, String> conf = new HashMap<String, String>();
			optree.getNode("Optimization").toKeyValues("", conf );
			//System.out.println("sadfas: "+conf);
			performanceModel.configureClassifier(conf);
			models.put(e.getKey(), performanceModel);
		}
	}


	public void writeCSVfileUniformSampleOfModel(String variable, Double samplingRate, String filename, String delimiter, boolean addPredicted) throws Exception{

        File file = new File(filename);
    	FileOutputStream fos = new FileOutputStream(file);
    	 
    	BufferedWriter writter = new BufferedWriter(new OutputStreamWriter(fos));
    	getUniformSampleOfModel(variable, samplingRate, writter, delimiter, addPredicted);

    	writter.close();
		
	}
	
	public void writeCSVfileUniformSampleOfModel(String variable, Double samplingRate, String filename, String delimiter) throws Exception{

        File file = new File(filename);
    	FileOutputStream fos = new FileOutputStream(file);
    	 
    	BufferedWriter writter = new BufferedWriter(new OutputStreamWriter(fos));
    	getUniformSampleOfModel(variable, samplingRate, writter, delimiter, false);

    	writter.close();
	}
	
	protected void getUniformSampleOfModel(String variable, Double samplingRate, BufferedWriter writter, String delimiter, boolean addPredicted) throws Exception{
		
		Model m = models.get(variable);
		Sampler s = (Sampler) new UniformSampler();
        s.setSamplingRate(samplingRate);
    	HashMap<String, List<Double>> dim = new HashMap<String, List<Double>>();
        for(Entry<String, String> e : m.getInputSpace().entrySet()){
            writter.append(e.getKey()+delimiter);
        	String[] limits = e.getValue().split(delimiter);
        	List<Double> l = new ArrayList<Double>();
        	Double min = Double.parseDouble(limits[1]);
        	Double max = Double.parseDouble(limits[2]);
        	if(limits[3].startsWith("l")){
        		Double step = 10.0;
        		for (double i = min; i <= max; i*=step) {
        			l.add(i);
				}
        	}
        	else{
        		Double step = Double.parseDouble(limits[3]);
        		for (double i = min; i <= max; i+=step) {
        			l.add(i);
				}
        	}
        	dim.put(e.getKey(), l);
        }
        int i=0;
        for(String k : m.getOutputSpace().keySet()){
            writter.append(k);
        	i++;
        	if(i<m.getOutputSpace().size()){
                writter.append(delimiter);
        	}
        }
        if(addPredicted){
            writter.append(delimiter+"prediction");
        }
        writter.newLine();
        //System.out.println(dim);
		s.setDimensionsWithRanges(dim);
        s.configureSampler();
        while (s.hasMore()) {
            InputSpacePoint nextSample = s.next();
    		OutputSpacePoint op =  new 	OutputSpacePoint();
            HashMap<String, Double> values = new HashMap<String, Double>();
            for(String k :  m.getOutputSpace().keySet()){
            	values.put(k, null);
            }
            op.setValues(values);
            //System.out.println(nextSample);
            OutputSpacePoint res = m.getPoint(nextSample,op);
            //System.out.println(res);
            writter.append(res.toCSVString(delimiter));
            if(addPredicted){
                writter.append(delimiter+"true");
            }
            writter.newLine();
        }
        
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
	
	public void readFromDir() throws Exception{
		//System.out.println("operator: "+opName);
        File f = new File(directory+"/description");
		InputStream stream = new FileInputStream(f);
		Properties props = new Properties();
		props.load(stream);
		for(Entry<Object, Object> e : props.entrySet()){
			add((String)e.getKey(), (String)e.getValue());
		}
		stream.close();
		configureModel();
		
		//this.performanceModel = AbstractWekaModel.readFromFile(directory+"/model");
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
        for(Entry<String, Model> e : models.entrySet()){
        	e.getValue().serialize(directory+"/"+e.getKey()+".model");
        }
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
//		Operator op = new Operator("HBase_HashJoin");
//		op.add("Constraints.Input.number","2");
//		op.add("Constraints.Output.number","1");
//		op.add("Constraints.Input0.DataInfo.Attributes.number","2");
//		op.add("Constraints.Input0.DataInfo.Attributes.Atr1.type","ByteWritable");
//		op.add("Constraints.Input0.DataInfo.Attributes.Atr2.type","List<ByteWritable>");
//		op.add("Constraints.Input0.Engine.DB.NoSQL.HBase.key","Atr1");
//		op.add("Constraints.Input0.Engine.DB.NoSQL.HBase.value","Atr2");
//		op.add("Constraints.Input0.Engine.DB.NoSQL.HBase.location","127.0.0.1");
//
//		op.add("Constraints.Input1.DataInfo.Attributes.number","2");
//		op.add("Constraints.Input1.DataInfo.Attributes.Atr1.type","ByteWritable");
//		op.add("Constraints.Input1.DataInfo.Attributes.Atr2.type","List<ByteWritable>");
//		op.add("Constraints.Input1.Engine.DB.NoSQL.HBase.key","Atr1");
//		op.add("Constraints.Input1.Engine.DB.NoSQL.HBase.value","Atr2");
//		op.add("Constraints.Input1.Engine.DB.NoSQL.HBase.location","127.0.0.1");
//
//		op.add("Constraints.Output0.DataInfo.Attributes.number","2");
//		op.add("Constraints.Output0.DataInfo.Attributes.Atr1.type","ByteWritable");
//		op.add("Constraints.Output0.DataInfo.Attributes.Atr2.type","List<ByteWritable>");
//		op.add("Constraints.Output0.Engine.DB.NoSQL.HBase.key","Atr1");
//		op.add("Constraints.Output0.Engine.DB.NoSQL.HBase.value","Atr2");
//		op.add("Constraints.Output0.Engine.DB.NoSQL.HBase.location","127.0.0.1");
//		
//		op.add("Constraints.OpSpecification.Algorithm.Join.JoinCondition","in1.atr1 = in2.atr2");
//		op.add("Constraints.OpSpecification.Algorithm.Join.type", "HashJoin");
//
//		op.add("Constraints.EngineSpecification.Distributed.MapReduce.masterLocation", "127.0.0.1");
//
//		op.add("Optimization.model", "gr.ntua.ece.cslab.panic.core.models.UserFunction");
//		op.add("Optimization.inputSpace.In0.uniqueKeys", "Double,1.0,1E10,l");
//		op.add("Optimization.inputSpace.In1.uniqueKeys", "Double,1.0,1E10,l");
//		op.add("Optimization.inputSpace.cores", "Double,1.0,40.0,5.0");
//		op.add("Optimization.outputSpace.execTime", "Double");
//		op.add("Optimization.outputSpace.Out0.uniqueKeys", "Integer");
//		op.add("Optimization.execTime", "100.0 + (In0.uniqueKeys + In1.uniqueKeys)/cores");
//		op.add("Optimization.Out0.uniqueKeys", "In0.uniqueKeys + In1.uniqueKeys");
		Operator op = new Operator("HBase_HashJoin","/Users/npapa/Documents/workspace/asap/asapLibrary/operators/Sort");

		op.readFromDir();
		op.writeCSVfileUniformSampleOfModel("Out0.size", 1.0, "test.csv", ",");
		
		//op.configureModel();
		//op.writeCSVfileUniformSampleOfModel(0.2, "test.csv", ",");
		
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

		Operator op1 = new Operator("HBase_HashJoin", "/tmp");
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
