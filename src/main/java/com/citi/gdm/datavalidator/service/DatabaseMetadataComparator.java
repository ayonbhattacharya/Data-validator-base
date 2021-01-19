/**
 *
 */
package com.citi.gdm.datavalidator.service;

import com.citi.gdm.datavalidator.domain.DatabaseMetadataSummaryReport;

/**
 * @author ayon.bhattacharya
 *
 */
public interface DatabaseMetadataComparator {

	public DatabaseMetadataSummaryReport compareDatabaseMetadata();

	public String getMappedDatatype(String key, String defaultValue);
}
