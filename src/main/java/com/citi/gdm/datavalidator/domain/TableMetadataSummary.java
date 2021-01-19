package com.citi.gdm.datavalidator.domain;

import org.apache.commons.lang.builder.ToStringBuilder;

/**
 * 
 * @author ayon.bhattacharya
 *
 */
public class TableMetadataSummary {


	/**
	 *
	 */
	public TableMetadataSummary() {
	}

	private String sourceColName;
	private String sourceDatatype;

	private String targetColName;
	private String targetDatatype;

	public String getSourceColName() {
		return sourceColName;
	}

	public void setSourceColName(String sourceColName) {
		this.sourceColName = sourceColName;
	}

	public String getSourceDatatype() {
		return sourceDatatype;
	}

	public void setSourceDatatype(String sourceDatatype) {
		this.sourceDatatype = sourceDatatype;
	}

	public String getTargetColName() {
		return targetColName;
	}

	public void setTargetColName(String targetColName) {
		this.targetColName = targetColName;
	}

	public String getTargetDatatype() {
		return targetDatatype;
	}

	public void setTargetDatatype(String targetDatatype) {
		this.targetDatatype = targetDatatype;
	}

	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this);
	}

}
