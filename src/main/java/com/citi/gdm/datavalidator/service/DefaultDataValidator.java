package com.citi.gdm.datavalidator.service;

public interface DefaultDataValidator {

	/* 
	  * This method will start the DAta Validation process
	  * 1. Read source database metadata information
	  * 2. Caches the metadata information
	  * 3. Fetches metadata from hawq
	  * 4. Compares the metadata
	  * 5. Generates abmormality report
	  * 6. Compares actual data
	  * 7. Generates abnormality report
	  */
	
	public void startTeradata2HawqValidation();
}
