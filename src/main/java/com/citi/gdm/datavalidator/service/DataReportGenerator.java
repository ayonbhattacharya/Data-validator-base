package com.citi.gdm.datavalidator.service;

import com.citi.gdm.datavalidator.domain.DatabaseMetadataSummary;
import com.citi.gdm.datavalidator.domain.DatabaseMetadataSummaryReport;

/**
 * 
 * @author ayon.bhattacharya
 *
 */
public interface DataReportGenerator {

	public void generateDataReport(DatabaseMetadataSummaryReport report);
	public boolean isDataComparisionRequired(DatabaseMetadataSummary mdSummary);
}
