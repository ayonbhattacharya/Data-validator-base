package com.citi.gdm.datavalidator.service.impl;

import java.io.FileInputStream;

import javax.xml.transform.stream.StreamSource;


import org.datanucleus.store.exceptions.DatastoreValidationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.oxm.jaxb.Jaxb2Marshaller;
import org.springframework.stereotype.Service;

import com.citi.gdm.datavalidator.service.XmlToJavaConverter;


/**
 * 
 * @author ayon.bhattacharya
 *
 */

@Service
public class XmlToJavaConverterImpl<T> implements XmlToJavaConverter<T>{
	
	private static final Logger LOGGER =  LoggerFactory.getLogger(XmlToJavaConverterImpl.class);
	
	@Autowired
	private Jaxb2Marshaller jaxb2Marshaller;
	
	
	public XmlToJavaConverterImpl() {
	}
	@SuppressWarnings("unchecked")
	@Override
	public T convertFromXMLToObject(String xmlfile) {
		try(FileInputStream is = new FileInputStream(xmlfile);){
			return (T) jaxb2Marshaller.unmarshal(new StreamSource(is));
		}catch(Exception e ){
			LOGGER.error("Error while reading xml file {}, root cause is {}", xmlfile, e.getMessage());
			LOGGER.error(e.getMessage(),e);
			throw new DatastoreValidationException(e.getMessage(),e);
			
		}
	
	}

}
