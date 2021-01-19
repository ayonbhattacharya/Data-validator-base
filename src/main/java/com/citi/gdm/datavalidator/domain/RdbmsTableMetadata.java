package com.citi.gdm.datavalidator.domain;

import java.util.List;

import org.apache.commons.lang.builder.ToStringBuilder;

public class RdbmsTableMetadata extends TableMetadata {

	/**
	 *
	 */
	public RdbmsTableMetadata() {
	}

	private String schema;
	
	private String targetName;

	// It is possible to have composite primary key
	private List<String> pkList;


	public String getSchema() {
		return schema;
	}

	public void setSchema(String schema) {
		this.schema = schema;
	}

	public String getTargetName() {
		return targetName;
	}

	public void setTargetName(String targetName) {
		this.targetName = targetName;
	}

	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this);
	}

	public List<String> getPkList() {
		return pkList;
	}

	public void setPkList(List<String> pkList) {
		this.pkList = pkList;
	}
	
	private String columnTypeCheckRequired;
	private String additionalColumns;


	public String getColumnTypeCheckRequired() {
		return columnTypeCheckRequired;
	}

	public void setColumnTypeCheckRequired(String columnTypeCheckRequired) {
		this.columnTypeCheckRequired = columnTypeCheckRequired;
	}

	public String getAdditionalColumns() {
		return additionalColumns;
	}

	public void setAdditionalColumns(String additionalColumns) {
		this.additionalColumns = additionalColumns;
	}

}

