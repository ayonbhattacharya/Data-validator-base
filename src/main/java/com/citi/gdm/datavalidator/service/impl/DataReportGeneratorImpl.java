package com.citi.gdm.datavalidator.service.impl;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;

import org.apache.commons.lang.StringUtils;
import org.apache.velocity.app.VelocityEngine;
import org.apache.velocity.tools.generic.MathTool;
import org.apache.velocity.tools.generic.NumberTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.ui.velocity.VelocityEngineUtils;

import com.citi.gdm.datavalidator.data.store.AppDataStore;
import com.citi.gdm.datavalidator.domain.Column;
import com.citi.gdm.datavalidator.domain.DatabaseMetadataSummary;
import com.citi.gdm.datavalidator.domain.DatabaseMetadataSummaryReport;
import com.citi.gdm.datavalidator.domain.RdbmsTableMetadata;
import com.citi.gdm.datavalidator.domain.TableDataReport;
import com.citi.gdm.datavalidator.domain.TableMetadata;
import com.citi.gdm.datavalidator.service.DataReportGenerator;
import com.citi.gdm.datavalidator.util.Utils;

import microsoft.sql.DateTimeOffset;
@Service
public class DataReportGeneratorImpl implements DataReportGenerator{
	private static final Logger LOGGER = LoggerFactory.getLogger(DataReportGeneratorImpl.class);

	@Autowired
	private DataSource dataSource;

	@Autowired
	@Qualifier("hawqDataSource")
	private DataSource hawqDataSource;

	@Value("${db.table.column.exclude.list}")
	private String[] excludeCols;

	private List<String> excludeColList;

	@Value("${database.metadata.reports.folder.location}")
	private String databaseMetadataReportsFolderLocation;

	@Autowired
	private VelocityEngine velocityEngine;

	@Value("${default.row.count.to.validate}")
	private Integer defaultRowCountToValidate;

	@Value("${db.compare.data.for.metadata.mismatch}")
	private Boolean compareDataForMetadataMismatch;

	@Value("${client.name}")
	private String clientName;

	@PostConstruct
	public void excludeArrayToList() {
		excludeColList = Arrays.asList(excludeCols);
	}

	/**
	 *
	 */
	public DataReportGeneratorImpl() {
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see
	 * com.emc.ps.appmod.service.DataReportGenerator#generateDataReport(com.emc.
	 * ps.appmod.domain.DatabaseMetadataSummaryReport)
	 */
	@Override
	public void generateDataReport(DatabaseMetadataSummaryReport report) {

		List<DatabaseMetadataSummary> mdSummaryList = report.getMdSummary();
		int totalTblsSize = mdSummaryList.size();
		int index = 0;

		Iterator<DatabaseMetadataSummary> it = mdSummaryList.iterator();
		while (it.hasNext()) {
			DatabaseMetadataSummary mdSummary = it.next();

			String sourceTblName = mdSummary.getSourceTblName();
			
			String targetTableName = mdSummary.getTargetTblName();

			String schema = Utils.getSchemaName(sourceTblName);
			//String targetSchemaName = Utils.get

			LOGGER.info("hawq table name to compare with rdbms table, schema {}, rdbms table name {} and hawq table name {}", schema, sourceTblName, targetTableName);


			LOGGER.info("============================================================================");
			LOGGER.info("======COMPARING SOURCE table {} DATA with TARGET for TABLE {} : {} OF {} =============", sourceTblName, targetTableName,
					++index, totalTblsSize);
			LOGGER.info("============================================================================");
			try {

				// If there is no mismatch in meta data then only precede for data
				// comparison
				if (isDataComparisionRequired(mdSummary)) {

					TableDataReport tableDataReport = getTableDataReport(report, sourceTblName, targetTableName, mdSummary);
					LOGGER.info("TableDataReport for {} is {}", sourceTblName, tableDataReport);

					generateTableDataReport(tableDataReport);

				} else {
					LOGGER.warn("Not proceeding with data comparing as there is meta data mismatch for table {}",
							sourceTblName);
				}
			} catch (Exception e) {
				// log the error and continue for other tables
				LOGGER.error("Error while comparing data for source table {} with target table {} and root cause is {}", sourceTblName,targetTableName, e.getMessage());
				it.remove();

			}

		}

		/*for (DatabaseMetadataSummary mdSummary : mdSummaryList) {

			String sourceTblName = mdSummary.getSourceTblName();

			String schema = Utils.getSchemaName(sourceTblName);

			String temphawqTblName =sourceTblName;
			if (false == "dbo".equalsIgnoreCase(schema)) {
				temphawqTblName = new StringBuilder().append(schema).append("_").append(sourceTblName).toString();
				LOGGER.info("hawq table name to compare with rdbms table, schema {}, rdbms table name {} and hawq table name {}", schema, sourceTblName, temphawqTblName);
			}

			LOGGER.info("============================================================================");
			LOGGER.info("======COMPARING SOURCE table {} DATA with TARGET for TABLE {} : {} OF {} =============", sourceTblName, temphawqTblName,
					++index, totalTblsSize);
			LOGGER.info("============================================================================");
			try {

				// If there is no mismatch in meta data then only precede for data
				// comparison
				if (isDataComparisionRequired(mdSummary)) {

					TableDataReport tableDataReport = getTableDataReport(report, sourceTblName, mdSummary);
					LOGGER.info("TableDataReport for {} is {}", sourceTblName, tableDataReport);

					generateTableDataReport(tableDataReport);

				} else {
					LOGGER.warn("Not proceeding with data comparing as there is meta data mismatch for table {}",
							sourceTblName);
				}
			} catch (Exception e) {
				// log the error and continue for other tables
				LOGGER.error("Error while comparing data for source table {} with target table {} and root cause is {}", sourceTblName,temphawqTblName, e.getMessage());

			}

		}*/
		LOGGER.info("Completed generating data comparision report for all tables.");
	}

	private void generateTableDataReport(TableDataReport tableDataReport) {

		String name = tableDataReport.getName();
		LOGGER.info("Started generating data report for table {}.", name);

		Map<String, Object> model = new HashMap<>();
		model.put("clientName", clientName);
		model.put("report", tableDataReport);
		model.put("numberTool", new NumberTool());
		model.put("mathTool", new MathTool());
		model.put("StringUtils", new StringUtils());

		String mdRepDirPath = AppDataStore.getReportFolder();
		if (StringUtils.isBlank(mdRepDirPath)) {
			databaseMetadataReportsFolderLocation = databaseMetadataReportsFolderLocation.endsWith(File.separator)
					? databaseMetadataReportsFolderLocation : databaseMetadataReportsFolderLocation + File.separator;
			mdRepDirPath = new StringBuilder().append(databaseMetadataReportsFolderLocation)
					.append(new SimpleDateFormat("dd-MM-yyyy_HH-mm-ss").format(new Date())).toString();
			AppDataStore.setReportFolder(mdRepDirPath);
		}

		File mdRepDir = new File(mdRepDirPath);
		if (false == mdRepDir.exists()) {
			mdRepDir.mkdir();
		}

		String mdRepFileName = new StringBuilder().append(mdRepDir.getPath()).append(File.separator).append(name)
				.append(".html").toString();
		Writer sw = new StringWriter();
		VelocityEngineUtils.mergeTemplate(velocityEngine, "data-summary.vm", "UTF-8", model, sw);
		try (FileWriter fw = new FileWriter(mdRepFileName);) {
			String reportContent = sw.toString();
			LOGGER.debug("Creating data report for table {} with file name {} with content {}.", name, mdRepFileName,
					reportContent);
			fw.write(reportContent);
		} catch (IOException e) {
			LOGGER.error("Error while creating data report for table {}, root cause is {}", name, e.getMessage());
			LOGGER.error(e.getMessage(), e);
			// do not throw exception , log and continue for next report
			//throw new DataValidatorException(e.getMessage(), e);
		}
		LOGGER.info("Started generating data report for table {}.", name);

	}

	private TableDataReport getTableDataReport(DatabaseMetadataSummaryReport report, String sourceTblName,
			String targetTableName, DatabaseMetadataSummary mdSummary) {
		String sourceSchemaName = Utils.getSchemaName(sourceTblName);

		LOGGER.info("Target table name to compare with rdbms table, schema {}, rdbms table name {} and hawq table name {}", sourceSchemaName, sourceTblName, targetTableName);



		Integer rowCountToValidate = Integer.parseInt(AppDataStore.getConfigTableInfo("dv_test1", sourceTblName)
				.getRowCountToValidate());
		JdbcTemplate rdbmsJdbcTemplate = new JdbcTemplate(dataSource);
		if (null == rowCountToValidate) {
			rowCountToValidate = defaultRowCountToValidate;
		}

		rdbmsJdbcTemplate.setMaxRows(rowCountToValidate);

		JdbcTemplate hawqJdbcTemplate = new JdbcTemplate(hawqDataSource);

		TableDataReport dataReport = new TableDataReport();
		List<String> colNames = new ArrayList<>();
		List<Map<String, Object>> matchRows = new ArrayList<>();
		List<Map<String, Object>> mismatchedRows = new ArrayList<>();
		// list to store map of column name and target data for mismatched
		// columns
		List<Map<String, Object>> mismatchedTargetRows = new ArrayList<>();

		dataReport.setName(sourceTblName);
		dataReport.setNoOfRowsCompared(rowCountToValidate);
		dataReport.setColNameTypeMap(getColNameTypeMap(sourceSchemaName, sourceTblName));

		int noOfMatchedRows = 0;
		int noOfMismatchedRows = 0;

		String rdbmsSql = new StringBuilder().append("select * from ").append("dv_test1").append(".").append(sourceTblName).toString();
		List<Map<String, Object>> rdbmsRows = rdbmsJdbcTemplate.queryForList(rdbmsSql);
		LOGGER.info("Executed query {} on rdbms , and response is {} ", rdbmsSql, rdbmsRows.toString());
		// get each record and compare it with target record
		RdbmsTableMetadata rdbmsTable = AppDataStore.getRdbmsTableInfo("dv_test1", sourceTblName);
		List<Column> rdbmsCols = rdbmsTable.getCols();
		for (Column column : rdbmsCols) {
			colNames.add(column.getName());
		}

		List<String> pkList = rdbmsTable.getPkList();
		Boolean isPkExists = (null != pkList) && (pkList.size() > 0) ? Boolean.TRUE : Boolean.FALSE;
		for (Map<String, Object> rdbmsRow : rdbmsRows) {
			try {
				// if primary key exists construct hawq query based on
				// primary key column
				if (isPkExists) {
					Map<String, Object> pkValueMap = new HashMap<>();
					for (String pk : pkList) {
						Object pkValue = rdbmsRow.get(pk);
						pkValueMap.put(pk, pkValue);
					}

					Set<Entry<String, Object>> entrySet = pkValueMap.entrySet();
					Iterator<Entry<String, Object>> it = entrySet.iterator();
					StringBuilder sb = new StringBuilder();
					Object[] pkValue = new Object[entrySet.size()];

					for (int i = 0;; i++) {
						Entry<String, Object> next = it.next();
						if (!it.hasNext()) {
							// work around to handle source columns with spaces in their name, in hawq these spaces are replaced with '_'
							sb.append(next.getKey().trim().replaceAll("\\s+", "_")).append("=?  ");
							pkValue[i] = next.getValue();
							break;
						}
						sb.append(next.getKey().trim().replaceAll("\\s+", "_")).append("=? and ");
						pkValue[i] = next.getValue();
					}
					//------ Works only if both schemas are same --------------------- 
					String schemaName = sourceSchemaName;
					String hawqQuery = new StringBuilder("select * from ").append("dv_test2").append(".").append(targetTableName).append(" ")
							.append("where ").append(sb.toString()).toString();

					StringBuilder paramData = new StringBuilder();
					for (Object o : pkValue) {
						paramData.append(o.toString()).append(",");
					}
					LOGGER.info(
							"Executing target query based on primary key...query is {} with params {}",
							hawqQuery, paramData.toString());

					// query hawq based on pk value
					List<Map<String, Object>> hawqRows = hawqJdbcTemplate.queryForList(hawqQuery, pkValue);

					LOGGER.info(
							"Executed target query based on primary key...query is {} with params {} and response is {}",
							hawqQuery, paramData.toString(), hawqRows);

					// if hawqRow is not empty, then compare rdbms row and
					// hawq row
					if ((null != hawqRows) && (hawqRows.size() > 0)) {
						for (Map<String, Object> hawqRow : hawqRows) {
							LOGGER.info("target row {} for source row {}", hawqRow, rdbmsRow);
							Map<String, Object> hawqMismatchedRow = new HashMap<>();
							// compare rdbms and hawq row
							Boolean isDataMatch = compareRdbmsandhawqRow(sourceTblName, targetTableName, rdbmsRow, hawqRow,
									hawqMismatchedRow);
							if (isDataMatch) {
								noOfMatchedRows++;
								matchRows.add(rdbmsRow);
							} else {
								noOfMismatchedRows++;
								mismatchedRows.add(rdbmsRow);
								mismatchedTargetRows.add(hawqMismatchedRow);
							}
							// break after finding one matched or unmatched
							// record...ideally we should get only one
							// record.
							break;
						}
					} else {
						noOfMismatchedRows++;
						mismatchedRows.add(rdbmsRow);
						Map<String, Object> hawqMismatchedRow = new HashMap<>();
						Set<Entry<String,Object>> es = rdbmsRow.entrySet();
						for (Entry<String, Object> e : es) {
							hawqMismatchedRow.put(e.getKey(),"N/A");
						}
						mismatchedTargetRows.add(hawqMismatchedRow);
						LOGGER.warn("No target row for rdbms row {}", rdbmsRow);
					}

				} else {

					Set<Entry<String, Object>> entrySet = rdbmsRow.entrySet();
					Iterator<Entry<String, Object>> it = entrySet.iterator();
					StringBuilder sb = new StringBuilder();
					
					StringBuilder paramData = new StringBuilder();
					
					List<Object> params = new ArrayList<Object>();

					while (true) {
						Entry<String, Object> next = it.next();
						if (!it.hasNext()) {
							// workaround for null object
							if (null != next.getValue()) {
								// work around to handle source columns with spaces in their name, in hawq these spaces are replaced with '_'
								sb.append(next.getKey().trim().replaceAll("\\s+", "_")).append("=?  ");
								
								/*if (next.getValue() instanceof Timestamp){
									//SCL work around
									//Datetime in sqlserver view is migrated as sting in hawq
									//Sqlserver has nanosecond values in higher precision but hawq has only 1 digit for nanosecond
									//trim the value from sql server so that only 1 digit is sent for nanosecond in hawq query.
									
									String timeString = next.getValue().toString();
									String modifiedTime = timeString;
									int index = timeString.indexOf(".");
									if (index > 0) {
										modifiedTime = timeString.substring(0, index+2);
									} 
									paramData.append(modifiedTime).append(",");
									params.add(modifiedTime);
									
								} else {
									paramData.append(next.getValue().toString()).append(",");
									params.add(next.getValue());
								}*/
								
								paramData.append(next.getValue().toString()).append(",");
								params.add(next.getValue());
								
							} else {
								//If last value is null, remove the 'and ' at the end of the query.
								
								sb = new StringBuilder(sb.substring(0, sb.length()-4));
							}
							break;
						}
						// workaround for null object
						if (null != next.getValue()) {
							sb.append(next.getKey().trim().replaceAll("\\s+", "_")).append("=? and ");
							
							/*if (next.getValue() instanceof Timestamp){
								//SCL work around
								//Datetime in sqlserver view is migrated as sting in hawq
								//Sqlserver has nanosecond values in higher precision but hawq has only 1 digit for nanosecond
								//trim the value from sql server so that only 1 digit is sent for nanosecond in hawq query.
								
								String timeString = next.getValue().toString();
								String modifiedTime = timeString;
								int index = timeString.indexOf(".");
								if (index > 0) {
									modifiedTime = timeString.substring(0, index+2);
								} 
								paramData.append(modifiedTime).append(",");
								params.add(modifiedTime);
								
							} else {
								paramData.append(next.getValue().toString()).append(",");
								params.add(next.getValue());
							}*/
							
							paramData.append(next.getValue().toString()).append(",");
							params.add(next.getValue());
						}
					}
					
					Object[] colValue = params.toArray();

	/*				String hawqQuery = new StringBuilder("select * from ").append(targetTableName).append(" ")
							.append("where ").append(sb.toString()).toString();*/
					//---------- Changes made to insert schema name into query
					
					String hawqQuery = new StringBuilder("select * from ").append(sourceSchemaName).append(".").append(targetTableName).append(" ")
							.append("where ").append(sb.toString()).toString();

					
					LOGGER.info(
							"Executing target query based on all columns...query is {} with params {}",
							hawqQuery, paramData.toString());

					// query hawq based on pk value
					List<Map<String, Object>> hawqRows = hawqJdbcTemplate.queryForList(hawqQuery, colValue);

					LOGGER.info(
							"Executed target query based on all columns...query is {} with params {} and response is {}",
							hawqQuery, paramData.toString(), hawqRows);

					// if hawqRow is not empty, then compare rdbms row and
					// hawq row
					if ((null != hawqRows) && (hawqRows.size() > 0)) {
						for (Map<String, Object> hawqRow : hawqRows) {
							LOGGER.info("target row {} for source row {}", hawqRow, rdbmsRow);
							Map<String, Object> hawqMismatchedRow = new HashMap<>();
							// compare rdbms and hawq row
							Boolean isDataMatch = compareRdbmsandhawqRow(sourceTblName, targetTableName, rdbmsRow, hawqRow,
									hawqMismatchedRow);
							if (isDataMatch) {
								noOfMatchedRows++;
								matchRows.add(rdbmsRow);
							} else {
								noOfMismatchedRows++;
								mismatchedRows.add(rdbmsRow);
								mismatchedTargetRows.add(hawqMismatchedRow);
							}
							// break after finding one matched or unmatched
							// record...ideally we should get only one
							// record.
							break;
						}
					} else {
						noOfMismatchedRows++;
						mismatchedRows.add(rdbmsRow);
						Map<String, Object> hawqMismatchedRow = new HashMap<>();
						Set<Entry<String,Object>> es = rdbmsRow.entrySet();
						for (Entry<String, Object> e : es) {
							hawqMismatchedRow.put(e.getKey(),"N/A");
						}
						mismatchedTargetRows.add(hawqMismatchedRow);
						LOGGER.warn("No target row for source row {}", rdbmsRow);
					}
				}
			} catch (Exception e) {
				noOfMismatchedRows++;
				mismatchedRows.add(rdbmsRow);
				Map<String, Object> hawqMismatchedRow = new HashMap<>();
				Set<Entry<String,Object>> es = rdbmsRow.entrySet();
				for (Entry<String, Object> ee : es) {
					hawqMismatchedRow.put(ee.getKey(),"N/A");
				}
				mismatchedTargetRows.add(hawqMismatchedRow);
				LOGGER.warn("No target row for source row {}", rdbmsRow);
				LOGGER.error("Error while comparing source date {} and root cause is {}", rdbmsRow, e.getMessage());
				LOGGER.error(e.getMessage(), e);
			}
		}

		dataReport.setNoOfMatchedRows(noOfMatchedRows);
		dataReport.setNoOfMismatchedRows(noOfMismatchedRows);
		Double percentageOfMismatch = (((double) noOfMismatchedRows) / (rowCountToValidate)) * 100;
		dataReport.setPercentageOfMismatch(percentageOfMismatch);
		dataReport.setColNames(colNames);
		dataReport.setMatchedRows(matchRows);
		dataReport.setMismatchedRows(mismatchedRows);
		dataReport.setMismatchedTargetRows(mismatchedTargetRows);

		mdSummary.setNoOfRowsCompared(dataReport.getNoOfRowsCompared());
		mdSummary.setNoOfMatchedRows(dataReport.getNoOfMatchedRows());
		mdSummary.setNoOfMismatchedRows(dataReport.getNoOfMismatchedRows());
		mdSummary.setPercentageOfMismatch(dataReport.getPercentageOfMismatch());
		mdSummary.setMatchStatus(getMatchStatus(mdSummary));

		return dataReport;
	}

	private String getMatchStatus(DatabaseMetadataSummary mdSummary) {
		String matchStatus = "MATCH";
		if (mdSummary.getNoOfMismatchedRows() > 0) {
			matchStatus = "<font color=red>MISMATCH</font>";
		}
		return matchStatus;
	}

	private Map<String, String> getColNameTypeMap(String schema, String sourceTblName) {
		Map<String, String> colNameTypeMap = new HashMap<>();
		RdbmsTableMetadata rdbmsTableInfo = AppDataStore.getRdbmsTableInfo(schema, sourceTblName);
		if (null != rdbmsTableInfo) {
			List<Column> cols = rdbmsTableInfo.getCols();
			for (Column column : cols) {
				colNameTypeMap.put(column.getName(), column.getType());
			}
		}
		return colNameTypeMap;
	}

	private Boolean compareRdbmsandhawqRow(String tblName, String targetTableName, Map<String, Object> rdbmsRow, Map<String, Object> hawqRow,
			Map<String, Object> hawqMismatchedRow) {

		LOGGER.info("Started comparing data...");
		if (rdbmsRow == hawqRow) {
			LOGGER.info("Data match due to same objects.");
			return true;
		}
		/*
		 * if (rdbmsRow.size() != (hawqRow.size() - excludeColList.size())) {
		 * return false; }
		 */
		try {
			Iterator<Entry<String, Object>> i = rdbmsRow.entrySet().iterator();
			while (i.hasNext()) {
				Entry<String, Object> e = i.next();
				// hawq returns all keys with lower case and with prefixed with
				// table name where as sql server
				// returns with camel case without table name
				String schema = Utils.getSchemaName(tblName);

				LOGGER.info("target table name to compare with source table, schema {}, rdbms table name {} and hawq table name {}", schema, tblName, targetTableName);
// ---------------------- ALERT commented out to match keys -------------------------------------------------
/*				String key = new StringBuilder().append(targetTableName.toLowerCase()).append(".")
						.append(e.getKey().toLowerCase()).toString();*/ 
// ---------------------- ALERT commented out to match keys -------------------------------------------------
				String key = new StringBuilder().append(e.getKey().toLowerCase()).toString();
				Object value = e.getValue();
				if (value == null) {
					if (!(((hawqRow.get(key) == null) || "null".equalsIgnoreCase(hawqRow.get(key).toString()))
							&& hawqRow.containsKey(key))) {
						LOGGER.info("Data mismatch for null value, key : {}, source value : {} and target value : {}", key,
								value, hawqRow.get(key));
						//return false;
						hawqMismatchedRow.put(e.getKey(), null != hawqRow.get(key) ? hawqRow.get(key).toString() : "null");
					}
				} else {
					if (!value.equals(hawqRow.get(key))) {
						Boolean isEqualIgnoreType = isEqualIgnoreType(value, hawqRow.get(key));
						if (isEqualIgnoreType) {
							continue;
						}
						LOGGER.info("Data mismatch for non-null value, key : {}, source value : {} and target value : {}",
								key, value, hawqRow.get(key));
						//return false;
						hawqMismatchedRow.put(e.getKey(), null != hawqRow.get(key) ? hawqRow.get(key).toString() : "null");
					}
				}
			}
		} catch (ClassCastException | NullPointerException unused) {
			LOGGER.info("Data mismatch due to error, root cause is {}", unused);
			//return false;
			// setting dummy object to handle data mismatch incase of any exception
			hawqMismatchedRow.put(new Object().toString(), new Object());
		}

		int hawqMismatchedColSize = hawqMismatchedRow.size();
		if (hawqMismatchedColSize == 0) {
			LOGGER.info("Data match.");
			return Boolean.TRUE;
		} else {
			LOGGER.info("Data mismatch and mismatched cols {}.", hawqMismatchedRow);
			return Boolean.FALSE;
		}
	}

	// This method is work around to handle due to the different types of types
	// for equal values as data is migrated changing data types
	private Boolean isEqualIgnoreType(Object source, Object target) {

		// logging for debugging purpose
		try {
			// try-catch is not required but added safe code execution
			// source will not be null at this stage
			LOGGER.info("source type {} and value {}", source.getClass().getName(), source);
			if (null != target) {
				LOGGER.info("target type {} and value {}", target.getClass().getName(), target);
			}
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		}

		//Workaround for SCL
		//If source is number and target is double, check on value
		if (((source instanceof BigDecimal) && (target instanceof Double))) {
			BigDecimal src = (BigDecimal)source;
			Double trg = (Double)target;
			LOGGER.info("Comparing numeric data by casting value, source {}, target {} ", source, target);
			 
			return trg.equals(Double.valueOf(src.doubleValue()));
			 
		}
		// handle all long -> string, double -> string, int -> string,
		// BigDecimal -> string, float -> string, short -> string,
		else if (((source instanceof Number) && (target instanceof String)) || ((source instanceof Number) && (target instanceof Number))) {
			LOGGER.info("Comparing numeric data ignoring type, source {}, target {} ", source, target);
			return source.toString().equals(target.toString());
		} // boolean -> string
		else if ((source instanceof Boolean) && (target instanceof String)) {
			LOGGER.info("Comparing boolean data ignoring type, source {}, target {} ", source, target);
			return source.toString().equals(target);
		} // byte[] -> string
		else if ((source instanceof byte[]) && (target instanceof String)) {
			LOGGER.info("Comparing byte array data ignoring type, source {}, target {} ", source, target);
			return source.toString().equals(target);
		} // date -> string, Timestamp -> string, time -> string
		else if ((source instanceof Date) && (target instanceof String)) {
			LOGGER.info("Comparing date data ignoring type, source {}, target {} ", source, target);
			//Workaround for SCL
			//Souce data has timestamp with nanosecond precision more than 1 digit, but hawq value has only 1 digit for nanosecond
			//trim source and compare
			
			String srcTime = source.toString();
			String modifiedSrcTime = srcTime;
			//reverting the workaround
			//int index = srcTime.indexOf(".");
			//if (index > 0) {
				//modifiedSrcTime = srcTime.substring(0,index+2);
			//}
			
			return modifiedSrcTime.equals(target);
		} //
		else if ((source instanceof DateTimeOffset) && (target instanceof String)) {
			LOGGER.info("Comparing sql server datetimeoffset data ignoring type, source {}, target {} ", source,
					target);
			// due to the issue in DateTimeOffset.toString method check date and
			// hours information and if match found return true.
			String[] _s_split = source.toString().split(":");
			String[] _t_split = target.toString().split(":");

			if ((_s_split.length > 0) && (_t_split.length > 0)) {
				return _s_split[0].equals(_t_split[0]);
			}

			/*
			 * boolean result = source.toString().equals(target); // if redirect
			 * object comparison (i.e., toString) is true no need to // format
			 * and compare, if equals comparison fails then format and //
			 * Compare if (result) { return true; }
			 * SDF.setTimeZone(TimeZone.getTimeZone("GMT")); DateTimeOffset d =
			 * (DateTimeOffset) source; Date date = new
			 * Date(d.getTimestamp().getTime()); return
			 * SDF.format(date).replace("+0000", "+00:00").equals(target);
			 */
		}
		LOGGER.info("Data mismatch even ignoring the type, source {}, target {}", source,
				null != target ? target.toString() : "");
		return Boolean.FALSE;

	}

	@Override
	public boolean isDataComparisionRequired(DatabaseMetadataSummary mdSummary) {
		// first check for configuration value whether to proceed for data
		// comparison or not.
		if ((null == compareDataForMetadataMismatch) || (Boolean.TRUE == compareDataForMetadataMismatch)) {
			return Boolean.TRUE;
		}

		// Ideally if there are not exclude columns then col count percentage
		// gap and col type percentage gap should be zero to conculde there in
		// no metadata mismatch
		// return
		// Double.valueOf(0d).equals(mdSummary.getColCountPercentageGap())
		// &&Double.valueOf(0d).equals(mdSummary.getColTypePercentageGap());

		// Work around to exclude dbtype and year columns created in
		// hawq
		String targetTblName = mdSummary.getTargetTblName();
		Integer sourceColCount = mdSummary.getSourceTblColCount();
		Integer targetColCount = mdSummary.getTargetTbleColCount();
		TableMetadata hawqTableInfo = AppDataStore.getHawqTableInfo(targetTblName);
		List<Column> cols = hawqTableInfo.getCols();
		List<String> colNameList = new ArrayList<>();
		for (Column column : cols) {
			colNameList.add(column.getName());
		}
		// if col count diff equals to configured excluded column count and hawq
		// col list should contain all excluded list and db type gap is zero
		// then we can consider data comparison is required
		LOGGER.info("***Exclude list is {}", excludeColList);
		return ((targetColCount - sourceColCount) == excludeColList.size()) && colNameList.containsAll(excludeColList)
				&& (mdSummary.getDbTypeGap() == excludeColList.size());
	}
}
