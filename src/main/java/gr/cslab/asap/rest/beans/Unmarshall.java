package gr.cslab.asap.rest.beans;

import java.io.File;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

public class Unmarshall {
	
	public static WorkflowDictionary unmarshall(String xmlFile) throws JAXBException {
		File file = new File(xmlFile);
		JAXBContext jaxbContext = JAXBContext.newInstance(WorkflowDictionary.class);
 
		Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
		WorkflowDictionary d = (WorkflowDictionary) jaxbUnmarshaller.unmarshal(file);
		return d;
	}
}
