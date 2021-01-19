package com.citi.gdm.datavalidator.service.impl;

import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.citi.gdm.datavalidator.domain.DatabaseMetadataSummary;
import com.citi.gdm.datavalidator.domain.DatabaseMetadataSummaryReport;
import com.citi.gdm.datavalidator.domain.DatabaseMetadataSummaryReport.ExecutionStatus;
import com.citi.gdm.datavalidator.service.DataReportGenerator;
import com.citi.gdm.datavalidator.service.DatabaseMetadataComparator;
import com.citi.gdm.datavalidator.service.DatabaseMetadataReportGenerator;
import com.citi.gdm.datavalidator.service.DefaultDataValidator;
import com.citi.gdm.datavalidator.service.TargetMetadataReader;

/**
 * 
 * @author ayon.bhattacharya
 *
 */
@Service
public class DefaultTeradata2HawqValidator implements DefaultDataValidator{
	
	private static final Logger LOGGER = LoggerFactory.getLogger(DefaultTeradata2HawqValidator.class);
	
	@Autowired
	private DatabaseMetadataConfigFileReaderImpl databaseMetadataConfigFileReader;
	
	@Autowired
	private DatabaseMetadataReaderImpl databaseMetadataReader;
	
	@Autowired
	private TargetMetadataReader targetMetadataReader;

	@Autowired
	private DatabaseMetadataComparator databaseMetadataComparator;
	
	@Autowired
	private DatabaseMetadataReportGenerator mdRepGenerator;
	
	@Autowired
	private DataReportGenerator dataReportGenerator;
	
	@Override
	public void startTeradata2HawqValidation() {
		LOGGER.info("============================================================================");
		LOGGER.info("===================== APPLICATION START ====================================");
		LOGGER.info("============================================================================");
		LOGGER.info("Started Teradata to Hawq Data Validation process");
		// Reads xml configured database meta data information and Caches the metadata information
		databaseMetadataConfigFileReader.processDatabaseMetadataConfigFile();
		LOGGER.info("Finished reading xml configuration and caching metadata information");
		// Reads source database meta data information and Caches the metadata information
		databaseMetadataReader.processDatabaseMetadata();
		LOGGER.info("Finished processing Teradata metadata information");
		//Read hawq database meta data information and cache it
		targetMetadataReader.processHiveMetadata();
		LOGGER.info("Finished processing Hawq metadata information");
		DatabaseMetadataSummaryReport databaseMetadataSummaryReport = databaseMetadataComparator.compareDatabaseMetadata();
		LOGGER.info(databaseMetadataSummaryReport.toString());
		// compare and generate report for data
		dataReportGenerator.generateDataReport(databaseMetadataSummaryReport);
		// update number of tables information
		updateNoOfTblsInfo(databaseMetadataSummaryReport);
		// sort the meta data summary, this has to be done after data comparison
		List<DatabaseMetadataSummary> databaseMetadataSummaryList = databaseMetadataSummaryReport.getMdSummary();
		Collections.sort(databaseMetadataSummaryList, Collections.reverseOrder());
		// generate report for meta data
		mdRepGenerator.generateMetadataReport(databaseMetadataSummaryReport);

		LOGGER.info("Completed Rdbms to hive validation process.");
		
		
	}
	
	private void updateNoOfTblsInfo(DatabaseMetadataSummaryReport report) {
		Integer noOfDataMatchTbles = 0;
		Integer noOfDataMisMatchTbls = 0;
		List<DatabaseMetadataSummary> mdSummary = report.getMdSummary();
		for (DatabaseMetadataSummary md : mdSummary) {
			Integer noOfMismatchedRows = null != md.getNoOfMismatchedRows() ? md.getNoOfMismatchedRows() : 0;;
			if (noOfMismatchedRows == 0) {
				noOfDataMatchTbles++;
			}else {
				noOfDataMisMatchTbls++;
			}
		}
		report.setNoOfDataMatchTbls(noOfDataMatchTbles);
		report.setNoOfDataMisMatchTbls(noOfDataMisMatchTbls);
		if (noOfDataMisMatchTbls > 0) {
			report.setStatus(ExecutionStatus.FAILURE);
		}
	}

}
