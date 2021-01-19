package com.citi.gdm.datavalidator.service;

//import com.emc.ps.appmod.exception.DataValidatorException;

/**
 * 
 * @author ayon.bhattacharya
 *
 */
public interface DatabaseMetadataReader {

	/**
	 * This method reads the database metadata and caches the data
	 * Incase of any exception this method will throw DataValidatorException
	 */
	public void processDatabaseMetadata();
}
