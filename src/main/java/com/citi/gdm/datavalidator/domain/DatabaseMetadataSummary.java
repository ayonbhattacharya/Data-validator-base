package com.citi.gdm.datavalidator.domain;

import com.citi.gdm.datavalidator.domain.DatabaseMetadataSummary.ConnectionStatus;

/**
 * 
 * @author ayon.bhattacharya
 *
 */
public class DatabaseMetadataSummary implements Comparable<DatabaseMetadataSummary>{
   
	public DatabaseMetadataSummary(){
		
	}
	
	public enum ConnectionStatus {
		SUCCESS, FAILURE
	}
	
	private String sourceTblName;
	private String targetTblName;
	private Integer sourceTblColCount;
	private Integer targetTbleColCount;
	private Double colCountPercentageGap;
	private Double colTypePercentageGap;
	private ConnectionStatus status;
	private Integer dbTypeGap;
	private String reportLink;

	private Integer noOfRowsCompared;
	private Integer noOfMatchedRows;
	private Integer noOfMismatchedRows;
	private Double percentageOfMismatch = Double.valueOf(0D);

	private String matchStatus;
	
	
	
	public String getSourceTblName() {
		return sourceTblName;
	}



	public void setSourceTblName(String sourceTblName) {
		this.sourceTblName = sourceTblName;
	}



	public String getTargetTblName() {
		return targetTblName;
	}



	public void setTargetTblName(String targetTblName) {
		this.targetTblName = targetTblName;
	}



	public Integer getSourceTblColCount() {
		return sourceTblColCount;
	}



	public void setSourceTblColCount(Integer sourceTblColCount) {
		this.sourceTblColCount = sourceTblColCount;
	}



	public Integer getTargetTbleColCount() {
		return targetTbleColCount;
	}



	public void setTargetTbleColCount(Integer targetTbleColCount) {
		this.targetTbleColCount = targetTbleColCount;
	}



	public Double getColCountPercentageGap() {
		return colCountPercentageGap;
	}



	public void setColCountPercentageGap(Double colCountPercentageGap) {
		this.colCountPercentageGap = colCountPercentageGap;
	}



	public Double getColTypePercentageGap() {
		return colTypePercentageGap;
	}



	public void setColTypePercentageGap(Double colTypePercentageGap) {
		this.colTypePercentageGap = colTypePercentageGap;
	}



	public ConnectionStatus getStatus() {
		return status;
	}



	public void setStatus(ConnectionStatus status) {
		this.status = status;
	}



	public String getReportLink() {
		return reportLink;
	}



	public void setReportLink(String reportLink) {
		this.reportLink = reportLink;
	}



	public Double getPercentageOfMismatch() {
		return percentageOfMismatch;
	}



	public void setPercentageOfMismatch(Double percentageOfMismatch) {
		this.percentageOfMismatch = percentageOfMismatch;
	}



	@Override
	public int compareTo(DatabaseMetadataSummary o) {
		// Natural sorting order is compare first based on column type
		// percentage gap and then column date percentage gap
		int result = 0;
		if ((null != o.matchStatus) && (null != matchStatus) ) {
			result = o.matchStatus.compareTo(matchStatus);
			if (0 == result) {
				result = colTypePercentageGap.compareTo(o.colTypePercentageGap);
				if ((0 == result) && (null != percentageOfMismatch) && (null != o.percentageOfMismatch)) {
					result = percentageOfMismatch.compareTo(o.percentageOfMismatch);
				}
			}
		}
		return result;
	}



	public void setDbTypeGap(Integer dbTypeGap) {
		this.dbTypeGap = dbTypeGap;
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



	public String getMatchStatus() {
		return matchStatus;
	}



	public void setMatchStatus(String matchStatus) {
		this.matchStatus = matchStatus;
	}



	public Integer getDbTypeGap() {
		return dbTypeGap;
	}



}
