package com.citi.gdm.datavalidator.domain;

import java.util.Date;
import java.util.List;

import org.apache.commons.lang.builder.ToStringBuilder;

import com.citi.gdm.datavalidator.domain.DatabaseMetadataSummaryReport.ExecutionStatus;

/**
 * 
 * @author ayon.bhattacharya
 *
 */

public class DatabaseMetadataSummaryReport {
	
	public enum ExecutionStatus {
		SUCCESS, FAILURE
	}
	

	/**
	 *
	 */
	public DatabaseMetadataSummaryReport() {
	}

	private String sourceIp;
	private String targetIp;
	private String sourceDbName;
	private String targetDbName;
	private String sourceSchemaName;
	private String targetSchemaName = "N/A";
	private String sourceUserName;
	private String targetUserName;
	private Date runTime;
	private ExecutionStatus status;
	private String dbSystem;

	private List<DatabaseMetadataSummary> mdSummary;

	private List<TableMetadataSummaryReport> tableSummaryList;

	private Integer noOfDataMatchTbls;
	private Integer noOfDataMisMatchTbls;

	private List<String> nonExistingRdbmsTbls;
	private List<String> nonExistingHiveTbls;

	public String getSourceIp() {
		return sourceIp;
	}

	public void setSourceIp(String sourceIp) {
		this.sourceIp = sourceIp;
	}

	public String getTargetIp() {
		return targetIp;
	}

	public void setTargetIp(String targetIp) {
		this.targetIp = targetIp;
	}

	public String getSourceDbName() {
		return sourceDbName;
	}

	public void setSourceDbName(String sourceDbName) {
		this.sourceDbName = sourceDbName;
	}

	public String getTargetDbName() {
		return targetDbName;
	}

	public void setTargetDbName(String targetDbName) {
		this.targetDbName = targetDbName;
	}

	public String getSourceSchemaName() {
		return sourceSchemaName;
	}

	public void setSourceSchemaName(String sourceSchemaName) {
		this.sourceSchemaName = sourceSchemaName;
	}

	public String getTargetSchemaName() {
		return targetSchemaName;
	}

	public void setTargetSchemaName(String targetSchemaName) {
		this.targetSchemaName = targetSchemaName;
	}

	public String getSourceUserName() {
		return sourceUserName;
	}

	public void setSourceUserName(String sourceUserName) {
		this.sourceUserName = sourceUserName;
	}

	public String getTargetUserName() {
		return targetUserName;
	}

	public void setTargetUserName(String targetUserName) {
		this.targetUserName = targetUserName;
	}

	public Date getRunTime() {
		return runTime;
	}

	public void setRunTime(Date runTime) {
		this.runTime = runTime;
	}

	public ExecutionStatus getStatus() {
		return status;
	}

	public void setStatus(ExecutionStatus status) {
		this.status = status;
	}

	public String getDbSystem() {
		return dbSystem;
	}

	public void setDbSystem(String dbSystem) {
		this.dbSystem = dbSystem;
	}

	public List<DatabaseMetadataSummary> getMdSummary() {
		return mdSummary;
	}

	public void setMdSummary(List<DatabaseMetadataSummary> mdSummary) {
		this.mdSummary = mdSummary;
	}


	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this);
	}

	public List<TableMetadataSummaryReport> getTableSummaryList() {
		return tableSummaryList;
	}

	public void setTableSummaryList(List<TableMetadataSummaryReport> tableSummaryList) {
		this.tableSummaryList = tableSummaryList;
	}

	public List<String> getNonExistingRdbmsTbls() {
		return nonExistingRdbmsTbls;
	}

	public void setNonExistingRdbmsTbls(List<String> nonExistingRdbmsTbls) {
		this.nonExistingRdbmsTbls = nonExistingRdbmsTbls;
	}

	public List<String> getNonExistingHiveTbls() {
		return nonExistingHiveTbls;
	}

	public void setNonExistingHiveTbls(List<String> nonExistingHiveTbls) {
		this.nonExistingHiveTbls = nonExistingHiveTbls;
	}

	public Integer getNoOfDataMatchTbls() {
		return noOfDataMatchTbls;
	}

	public void setNoOfDataMatchTbls(Integer noOfDataMatchTbls) {
		this.noOfDataMatchTbls = noOfDataMatchTbls;
	}

	public Integer getNoOfDataMisMatchTbls() {
		return noOfDataMisMatchTbls;
	}

	public void setNoOfDataMisMatchTbls(Integer noOfDataMisMatchTbls) {
		this.noOfDataMisMatchTbls = noOfDataMisMatchTbls;
	}

}
