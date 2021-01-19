package com.citi.gdm.datavalidator.service;

/**
 * 
 * @author Ayon.bhattacharya
 *
 */


public interface DataBaseMetadataConfigFileReader {

	/**
	 * This method reads the database configuration xml file and caches it in memory
	 * In case of exception this method will throw DataValidatorException
	 */
	
	public void processDatabaseMetadataConfigFile();
}
