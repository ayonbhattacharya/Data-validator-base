package com.citi.gdm.datavalidator.domain;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang.builder.ToStringBuilder;

/**
 * 
 * @author ayon.bhattacharya
 *
 */

public class TableDataReport {

	public TableDataReport(){
		
	}
	
	private String name;
	private Integer noOfRowsCompared;
	private Integer noOfMatchedRows;
	private Integer noOfMismatchedRows;
	private Double percentageOfMismatch;

	private List<String> colNames;

	private List<Map<String, Object>> matchedRows;
	private List<Map<String, Object>> mismatchedRows;

	private Map<String, String> colNameTypeMap;

	private List<Map<String, Object>> mismatchedTargetRows;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Integer getNoOfRowsCompared() {
		return noOfRowsCompared;
	}

	public void setNoOfRowsCompared(Integer noOfRowsCompared) {
		this.noOfRowsCompared = noOfRowsCompared;
	}

	public Integer getNoOfMatchedRows() {
		return noOfMatchedRows;
	}

	public void setNoOfMatchedRows(Integer noOfMatchedRows) {
		this.noOfMatchedRows = noOfMatchedRows;
	}

	public Integer getNoOfMismatchedRows() {
		return noOfMismatchedRows;
	}

	public void setNoOfMismatchedRows(Integer noOfMismatchedRows) {
		this.noOfMismatchedRows = noOfMismatchedRows;
	}

	public Double getPercentageOfMismatch() {
		return percentageOfMismatch;
	}

	public void setPercentageOfMismatch(Double percentageOfMismatch) {
		this.percentageOfMismatch = percentageOfMismatch;
	}

	public List<String> getColNames() {
		return colNames;
	}

	public void setColNames(List<String> colNames) {
		this.colNames = colNames;
	}

	public List<Map<String, Object>> getMatchedRows() {
		return matchedRows;
	}

	public void setMatchedRows(List<Map<String, Object>> matchedRows) {
		this.matchedRows = matchedRows;
	}

	public List<Map<String, Object>> getMismatchedRows() {
		return mismatchedRows;
	}

	public void setMismatchedRows(List<Map<String, Object>> mismatchedRows) {
		this.mismatchedRows = mismatchedRows;
	}

	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this);
	}

	public Map<String, String> getColNameTypeMap() {
		return colNameTypeMap;
	}

	public void setColNameTypeMap(Map<String, String> colNameTypeMap) {
		this.colNameTypeMap = colNameTypeMap;
	}

	public List<Map<String, Object>> getMismatchedTargetRows() {
		return mismatchedTargetRows;
	}

	public void setMismatchedTargetRows(List<Map<String, Object>> mismatchedTargetRows) {
		this.mismatchedTargetRows = mismatchedTargetRows;
	}
	
	
}
