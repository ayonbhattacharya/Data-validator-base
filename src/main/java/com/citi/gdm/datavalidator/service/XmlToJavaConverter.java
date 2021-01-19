package com.citi.gdm.datavalidator.service;

//import com.emc.ps.appmod.exception.DataValidatorException;

/**
 * 
 * @author ayon.bhattacharya
 *
 */

public interface XmlToJavaConverter<T> {

	/**
	 * This method will read given xml files and unmarshals to java object.
	 * In case of any exception this method will throw {@link DataValidatorException}
	 */
	public T convertFromXMLToObject(String xmlfile) ;
}
