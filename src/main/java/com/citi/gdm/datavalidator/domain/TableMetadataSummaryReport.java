package com.citi.gdm.datavalidator.domain;

import java.util.List;

import org.apache.commons.lang.builder.ToStringBuilder;

public class TableMetadataSummaryReport {

	public TableMetadataSummaryReport() {
	}

	private String tblName;
	private List<TableMetadataSummary> report;

	public String getTblName() {
		return tblName;
	}

	public void setTblName(String tblName) {
		this.tblName = tblName;
	}

	public List<TableMetadataSummary> getReport() {
		return report;
	}

	public void setReport(List<TableMetadataSummary> report) {
		this.report = report;
	}

	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this);
	}
}
