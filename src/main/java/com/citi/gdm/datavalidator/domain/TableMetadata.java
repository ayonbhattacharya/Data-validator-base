package com.citi.gdm.datavalidator.domain;

import java.util.List;

import org.apache.commons.lang.builder.ToStringBuilder;

/**
 * 
 * @author ayon.bhattacharya
 *
 */

public class TableMetadata {

	/**
	 *
	 */
	public TableMetadata() {
	}

	private String name;
	private List<Column> cols;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public List<Column> getCols() {
		return cols;
	}

	public void setCols(List<Column> cols) {
		this.cols = cols;
	}

	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this);
	}

}
