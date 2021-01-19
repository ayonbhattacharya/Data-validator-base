package com.citi.gdm.datavalidator.exception;

/**
 * 
 * @author ayon.bhattacharya
 *
 */
public class DataValidatorException extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	/**
	 * 
	 */
	public DataValidatorException(){
	}
	
	public DataValidatorException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace){
		super(message, cause, enableSuppression, writableStackTrace);
	}
	public DataValidatorException(String message, Throwable cause){
		super(message, cause);
	}
	
	public DataValidatorException(String message) {
		super(message);
	}

	public DataValidatorException(Throwable cause) {
		super(cause);
	}

}
