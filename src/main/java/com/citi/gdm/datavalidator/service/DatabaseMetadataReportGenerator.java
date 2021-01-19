package com.citi.gdm.datavalidator.service;

import com.citi.gdm.datavalidator.domain.DatabaseMetadataSummaryReport;

/**
 * 
 * @author ayon.bhattacharya
 *
 */

public interface DatabaseMetadataReportGenerator {

	public void generateMetadataReport(DatabaseMetadataSummaryReport report);
}
