package com.citi.gdm.datavalidator.service.impl;



import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Service;

import com.citi.gdm.datavalidator.data.store.AppDataStore;
import com.citi.gdm.datavalidator.domain.Column;
import com.citi.gdm.datavalidator.domain.DatabaseMetadataSummary;
import com.citi.gdm.datavalidator.domain.DatabaseMetadataSummaryReport;
import com.citi.gdm.datavalidator.domain.RdbmsTableMetadata;
import com.citi.gdm.datavalidator.domain.TableMetadata;
import com.citi.gdm.datavalidator.domain.TableMetadataSummary;
import com.citi.gdm.datavalidator.domain.TableMetadataSummaryReport;
import com.citi.gdm.datavalidator.domain.DatabaseMetadataSummary.ConnectionStatus;
import com.citi.gdm.datavalidator.domain.DatabaseMetadataSummaryReport.ExecutionStatus;
import com.citi.gdm.datavalidator.exception.DataValidatorException;
import com.citi.gdm.datavalidator.service.DataReportGenerator;
import com.citi.gdm.datavalidator.service.DatabaseMetadataComparator;
import com.citi.gdm.datavalidator.util.Utils;

/**
 * 
 * @author ayon.bhattacharya
 *
 */
@Service
public class DatabaseMetadataComparatorImpl implements DatabaseMetadataComparator{
	
	private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseMetadataComparatorImpl.class);
	
	@Value("${database.data.type.mapping.file.location}")
	private Resource databaseDatatypeMappingFile;

	private final Properties databaseDatatypeMappingProperties = new Properties();

	@Value("${spring.sourceDatabase.system}")
	private String sourceDatasourceSystem;

	@Value("${spring.sourceDatabase.servername}")
	private String sourceDatasourceServername;

	@Value("${spring.sourceDatabase.databaseName}")
	private String sourceDatasourceDatabaseName;

	@Value("${spring.sourceDatabase.username}")
	private String sourceDatasourceUsername;

	@Value("${spring.targetDatabase.system}")
	private String targetDatasourceSystem;

	@Value("${spring.targetDatabase.servername}")
	private String targetDatasourceServername;

	@Value("${spring.targetDatabase.databaseName}")
	private String targetDatasourceDatabaseName;

	@Value("${spring.targetDatabase.username}")
	private String targetDatasourceUsername;
	
	@Value("${db.table.column.exclude.list}")
	private String excludedColumn;

	@Autowired
	private DataReportGenerator dataReportGenerator;

	@PostConstruct
	public void initDatatypeMappings() {
		try (InputStream inputStream = databaseDatatypeMappingFile.getInputStream();) {
			databaseDatatypeMappingProperties.load(inputStream);
			LOGGER.info("Read database datatype properties file. Datatype info is {}.",
					databaseDatatypeMappingProperties);
		} catch (IOException e) {
			LOGGER.error("Error while loading database datatype properties file, root cause is {}.", e.getMessage());
			LOGGER.error(e.getMessage(), e);
			throw new DataValidatorException(e.getMessage(), e);
		}
	}
	
	@Override
	public DatabaseMetadataSummaryReport compareDatabaseMetadata() {
		ExecutionStatus status = ExecutionStatus.SUCCESS;
		List<RdbmsTableMetadata> rdbmsTableInfo = AppDataStore.getRdbmsTableInfoList();
		DatabaseMetadataSummaryReport rep = new DatabaseMetadataSummaryReport();
		
		List<TableMetadataSummaryReport> tblMdSummaryReportList = new ArrayList<>();
		List<DatabaseMetadataSummary> databaseMetadataSummaryList = new ArrayList<>();
		
		int totalTblsSize = rdbmsTableInfo.size();
		int index = 0;
		
		for (RdbmsTableMetadata tableMetadata : rdbmsTableInfo) {
			String rdbmsTbleName = tableMetadata.getName();
			String targetTableName = tableMetadata.getTargetName();
			
			if (null == targetTableName || "".equals(targetTableName)){
				targetTableName = rdbmsTbleName;
			}

			String schema = Utils.getSchemaName(rdbmsTbleName);
			LOGGER.info("Hawq table name to compare with rdbms table, schema {}, rdbms table name {} and hawq table name {}", schema, rdbmsTbleName, targetTableName);
			TableMetadata hawqTableMetadata = AppDataStore.getHawqTableInfo(targetTableName);

			LOGGER.info("============================================================================");
			LOGGER.info("======COMPARING SOURCE METADATA for table {} with TARGET for TABLE {} : {} OF {} =============",
					rdbmsTbleName, targetTableName, ++index, totalTblsSize);
			LOGGER.info("============================================================================");
			
			if (null == hawqTableMetadata) {
				continue;
			}
			
			List<Column> rdbmsCols = tableMetadata.getCols();
			List<Column> hawqCols = hawqTableMetadata.getCols();
			String columnTypeCheckRequired = tableMetadata.getColumnTypeCheckRequired();
			// Start comparing columns
			TableMetadataSummaryReport tableSummary = new TableMetadataSummaryReport();
			tableSummary.setTblName(rdbmsTbleName);
			List<TableMetadataSummary> tblMdSummaryList = new ArrayList<>();
			
			List<String> rdbmsColNameList = new ArrayList();
			for(Column t : rdbmsCols){
				rdbmsColNameList.add(t.getName().toLowerCase());
			}
			List<String> hawqColNameList = new ArrayList();
			for(Column t : hawqCols){
				hawqColNameList.add(t.getName().toLowerCase());
			}
			
			// get column name mismatch list
			List<String> colNameUnionList = new ArrayList<>();
			for (String t : rdbmsColNameList) {
				if (hawqColNameList.contains(t)) {
					colNameUnionList.add(t);
				}
			}
			
			// then remove unique columns from all columns to get mismatched
			// columns
			
			
			List <String>excludedColumnArray = new ArrayList<>(Arrays.asList(excludedColumn.split(",")));
			
			Set<String> colNameMismatchSet = new HashSet<>();
			colNameMismatchSet.addAll(rdbmsColNameList);
			colNameMismatchSet.addAll(hawqColNameList);
			colNameMismatchSet.removeAll(colNameUnionList);
			colNameMismatchSet.removeAll(excludedColumnArray);
			LOGGER.info("Mismached column names {}", colNameMismatchSet);

			//--------------------------- ?? --------------------------------
			
			// get mismatched columns
			// first get unique columns
			List<Column> list = new ArrayList<>();
			for (Column t : rdbmsCols) {
				if (hawqCols.contains(t)) {
					list.add(t);
				}
			}
			// then remove unique columns from all columns to get mismatched
			// columns
			Set<Column> mismatchColSet = new HashSet<>();
			mismatchColSet.addAll(rdbmsCols);
			mismatchColSet.addAll(hawqCols);
			mismatchColSet.removeAll(list);
			LOGGER.info("Mismached columns {}", mismatchColSet);
			
			//--------------------------- ?? --------------------------------
			
			if (colNameMismatchSet.size() > 0) { // for column name mismatch
				status = ExecutionStatus.FAILURE;
				for (String colName : colNameMismatchSet) {
					Column rdbmsCol = null;
					Column hawqCol = null;
					// first get column for rdbms, and then hive column
					for (Column t : rdbmsCols) {
						if (t.getName().equalsIgnoreCase(colName)) {
							rdbmsCol = t;
							break;
						}
					}
					for (Column t : hawqCols) {
						if (t.getName().equalsIgnoreCase(colName)) {
							hawqCol = t;
							break;
						}
					}
					// if both found then its type mismatch
					
					if ((null != rdbmsCol) && (null != hawqCol)) {
						tblMdSummaryList.add(getTblMdSummary(rdbmsCol.getName(), rdbmsCol.getType(), hawqCol.getName(),
								hawqCol.getType()));
					}
					// if only found in rdbms then no column in hawq
					else if ((null != rdbmsCol) && (null == hawqCol)) {
						tblMdSummaryList.add(getTblMdSummary(rdbmsCol.getName(), rdbmsCol.getType(), "", ""));
					}
					// if only found in hawq then no column in rdbms
					else if ((null == rdbmsCol) && (null != hawqCol)) {
						tblMdSummaryList.add(getTblMdSummary("", "", hawqCol.getName(), hawqCol.getType()));
					}
				}//for close
		
			}/*else*/ if (mismatchColSet.size() > 0) { // for datatype mismatch
				status = ExecutionStatus.FAILURE;
				for (Column column : mismatchColSet) {

					if (colNameMismatchSet.contains(column.getName())) {
						continue;
					}
					Column rdbmsCol = null;
					Column hawqCol = null;
					String colName = column.getName();
					// first get column for rdbms, and then hawq column
					for (Column t1 : rdbmsCols) {
						if (t1.getName().equals(colName)) {
							rdbmsCol = t1;
							for (Column t2 : hawqCols) {
								if (t2.getName().equalsIgnoreCase(rdbmsCol.getName())) {
									hawqCol = t2;
									break;
								}
							}
							break;
						}
					}
					// if both found then its type mismatch
					if ((null != rdbmsCol) && (null != hawqCol)) {
						tblMdSummaryList.add(getTblMdSummary(rdbmsCol.getName(), rdbmsCol.getType(), hawqCol.getName(),
								hawqCol.getType()));
					}
				}
			}
			
			int tblMdSummaryListSize = tblMdSummaryList.size();
			tableSummary.setReport(tblMdSummaryList);
			tblMdSummaryReportList.add(tableSummary);
			//
			DatabaseMetadataSummary dbMdSummary = new DatabaseMetadataSummary();
			dbMdSummary.setSourceTblName(rdbmsTbleName);
			int rdbmsColsize = rdbmsCols.size();
			dbMdSummary.setSourceTblColCount(rdbmsColsize);
			dbMdSummary.setTargetTblName(targetTableName);
			int hiveColSize = hawqCols.size();
			dbMdSummary.setTargetTbleColCount(hiveColSize);
			Double colCountPercentageGap = rdbmsColsize == 0 ? 100D
					: (((double) (rdbmsColsize - hiveColSize) / rdbmsColsize) * 100);
			dbMdSummary.setColCountPercentageGap(colCountPercentageGap);
			Double colTypePercentageGap = rdbmsColsize == 0 ? 100D
					: (((double) tblMdSummaryListSize / rdbmsColsize) * 100);
			dbMdSummary.setColTypePercentageGap(colTypePercentageGap);
			dbMdSummary.setDbTypeGap(tblMdSummaryListSize);
			dbMdSummary.setStatus(ConnectionStatus.SUCCESS);
			String reportLink = "N/A";
			boolean dataComparisionRequired = dataReportGenerator.isDataComparisionRequired(dbMdSummary);
			if (dataComparisionRequired) {
				reportLink = new StringBuilder("<a href=\"").append(rdbmsTbleName).append(".html")
						.append("\" target=\"_blank\">").append(rdbmsTbleName).append(".html").append("</a>")
						.toString();
			}
			dbMdSummary.setReportLink(reportLink);
			databaseMetadataSummaryList.add(dbMdSummary);
		}
		rep.setTableSummaryList(tblMdSummaryReportList);
		rep.setMdSummary(databaseMetadataSummaryList);

		// update summary with source and target servers information
		updateSummary(rep);
		rep.setStatus(status);
		return rep;
	}

	@Override
	public String getMappedDatatype(String key, String defaultValue) {
		return databaseDatatypeMappingProperties.getProperty(key, defaultValue);
	}
	
	private TableMetadataSummary getTblMdSummary(String sourceColName, String sourceColType, String tragetColName,
			String targetColType) {
		TableMetadataSummary tblMdSummary = new TableMetadataSummary();
		tblMdSummary.setSourceColName(sourceColName);
		tblMdSummary.setSourceDatatype(sourceColType);
		tblMdSummary.setTargetColName(tragetColName);
		tblMdSummary.setTargetDatatype(targetColType);
		return tblMdSummary;
	}

	private void updateSummary(DatabaseMetadataSummaryReport rep) {
		// source database information
		// IP and Source system
		rep.setSourceIp(new StringBuilder().append(sourceDatasourceServername).append(", ")
				.append(sourceDatasourceSystem).toString());
		// Database name
		rep.setSourceDbName(sourceDatasourceDatabaseName);
		// Database system
		rep.setDbSystem(sourceDatasourceSystem);
		// Schema name
		List<RdbmsTableMetadata> rdbmsTableInfoList = AppDataStore.getRdbmsTableInfoList();
		String schema = Utils.getSourceSchemaNameForReport(rdbmsTableInfoList);
		rep.setSourceSchemaName(schema);
		// User name
		rep.setSourceUserName(sourceDatasourceUsername);
		// Runtime
		rep.setRunTime(new Date());
		// target database information
		// IP and source system
		rep.setTargetIp(new StringBuilder().append(targetDatasourceServername).append(", ")
				.append(targetDatasourceSystem).toString());
		// database name
		rep.setTargetDbName(targetDatasourceDatabaseName);
		// schema name
		rep.setTargetSchemaName("N/A");
		// user name
		rep.setTargetUserName(targetDatasourceUsername);

		// set non existing rdbms tables
		rep.setNonExistingRdbmsTbls(AppDataStore.getNonExistingRdbmsTbls());

		// set non existing hive tables
		rep.setNonExistingHiveTbls(AppDataStore.getNonExistingHawqTbls());
	}


}
