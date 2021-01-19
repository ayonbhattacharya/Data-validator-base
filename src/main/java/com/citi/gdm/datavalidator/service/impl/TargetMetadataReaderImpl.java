package com.citi.gdm.datavalidator.service.impl;


import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.AbstractMap.SimpleEntry;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.stereotype.Service;

import com.citi.gdm.datavalidator.data.store.AppDataStore;
import com.citi.gdm.datavalidator.domain.Column;
import com.citi.gdm.datavalidator.domain.RdbmsTableMetadata;
import com.citi.gdm.datavalidator.domain.TableInfo;
import com.citi.gdm.datavalidator.domain.TableMetadata;
import com.citi.gdm.datavalidator.exception.DataValidatorException;
import com.citi.gdm.datavalidator.service.DatabaseMetadataComparator;
import com.citi.gdm.datavalidator.service.TargetMetadataReader;

@Service
public class TargetMetadataReaderImpl implements TargetMetadataReader{
	
	private static final Logger LOGGER = LoggerFactory.getLogger(TargetMetadataReader.class);

	@Autowired
	@Qualifier("hawqDataSource")
	private DataSource targetDataSource;
	
	@Value("${db.compare.metadata.values}")
	private String compareMetadataFlag;
/*	@Autowired
	private DatabaseMetadataComparator databaseMetadataComparator;*/
	
	private static final String COLUMN_CHECK_DEFAULT = "TRUE";
	
	@Override
	public void processHiveMetadata() {
			Connection con =null;
			try{
				con= DataSourceUtils.getConnection(targetDataSource);
			}catch(CannotGetJdbcConnectionException e ){
				e.printStackTrace();
			}
			try{
				List<String> nonExistingHawqTables = new ArrayList();
				//get rdbms table information
				List<RdbmsTableMetadata> rdbmsTableInfoList = AppDataStore.getRdbmsTableInfoList();
				//hawq table metadata information
				List<TableMetadata> hawqMetadataList = new ArrayList<>();
				int totalTblsSize = rdbmsTableInfoList.size();
				int index =0;
				for (RdbmsTableMetadata rdbmsTableMetadata : rdbmsTableInfoList) {
					String schema = rdbmsTableMetadata.getSchema();
					String tableName = rdbmsTableMetadata.getName();
					String targetTableName = rdbmsTableMetadata.getTargetName();
					
					if (null == targetTableName || "".equals(targetTableName)) {
						targetTableName = tableName;
					}
					
					LOGGER.info("Target table name to compare with rdbms table, schema {}, source table name {} and target table name {}", schema, tableName, targetTableName);

					LOGGER.info("============================================================================");
					LOGGER.info("======READING TARGET METADATA for TABLE {} : {} OF {} =============", targetTableName, ++index,
							totalTblsSize);
					LOGGER.info("============================================================================");
				
					Statement stmt = con.createStatement();
					// describe table
					//String sql = new StringBuilder().append("describe ").append(targetTableName).toString();
					//String schema_name ="validator_test";
					String countSql = new StringBuilder().append("select count(*) as count from INFORMATION_SCHEMA.COLUMNS where table_name = \'").append(targetTableName).append("\';").toString();
					String sql = new StringBuilder().append("select * from INFORMATION_SCHEMA.COLUMNS where table_name = \'").append(targetTableName).append("\';").toString();
					LOGGER.info("Running query {} ", sql);
					ResultSet res = null;
					ResultSet res1 = null;
					try {
						res1 = stmt.executeQuery(countSql);
						res1.next();
						int rowcount = res1.getInt("count");
						if(rowcount==0){
							nonExistingHawqTables.add(targetTableName);
							LOGGER.error("Table with name {} does not exists in target database", targetTableName);
							continue;
						}
						res = stmt.executeQuery(sql);
					} catch (SQLException e) {
						e.printStackTrace();
						LOGGER.error("Error while describing table {}, root cause is {}", targetTableName, e.getMessage());
						continue;
					}
					TableMetadata hawqTblMetadata = new TableMetadata();
					hawqTblMetadata.setName(targetTableName);
					List<Column> colList = new ArrayList<>();
					SimpleEntry<String, String> tableEntry = new SimpleEntry<String, String>(schema, tableName);
					RdbmsTableMetadata rdbmsTableInfo = AppDataStore.getRdbmsTableInfo(tableEntry);
					List<Column> rdbmsCols = rdbmsTableInfo.getCols();
				
					while (res.next()) {
						String hawqColName = res.getString("column_name");
						String hawqType = null;
						if(compareMetadataFlag.equalsIgnoreCase("false")){
							hawqType = "Default";
						}
						else{
						 hawqType=res.getString("data_type");
						}
						for (Column column : rdbmsCols) {
							
							if (column.getName().equalsIgnoreCase(hawqColName)) {
								String rdbmsType = column.getType();
								hawqType = hawqType.indexOf("(") > 0 ? hawqType.substring(0, hawqType.indexOf("("))
										: hawqType;
								String mappingHawqType = "";
								//String mappingHiveType = databaseMetadataComparator.getMappedDatatype(rdbmsType, rdbmsType);
								if (hawqType.equalsIgnoreCase(mappingHawqType)) {
									hawqType = rdbmsType;
									break;
								}
							}
						}
						colList.add(new Column(hawqColName, hawqType));
				}
				
					hawqTblMetadata.setCols(colList);
					hawqMetadataList.add(hawqTblMetadata);
				}
				LOGGER.info("Extracted TARGET tables metadata and TARGET tables metadata is {}.", hawqMetadataList);
				// update cache
				AppDataStore.updateHawqMetadataCache(hawqMetadataList);
				// update non existing hawq tables
				AppDataStore.updateNonExistingHawqTbls(nonExistingHawqTables);
			}catch(Exception e1){
				LOGGER.error("Error while reading TARGET metadata for data source {}, root cause is {}", targetDataSource,e1.getMessage());
				LOGGER.error(e1.getMessage(), e1);
				throw new DataValidatorException(e1.getMessage(), e1);
			}
	}
	
	@Override
	public void processTargetDatabaseMetadata() {
		LOGGER.info("Started processing database Metadata");
		try {
			targetDataSource.setLoginTimeout(30);
		}catch(Exception e){
			LOGGER.debug("Login Timeout Exception");
			e.printStackTrace();
		}
		
		Connection con = DataSourceUtils.getConnection(targetDataSource);
		try{
			DatabaseMetaData md = con.getMetaData();
			List<String> nonExistingRdbmsTables = new ArrayList();
			List<TableInfo> tableInfoList = AppDataStore.getConfigTableInfoList();
			int totalTableSize = tableInfoList.size();
			int index =0;
			for(TableInfo tableInfo:tableInfoList){
				String schema = tableInfo.getSourceSchema();
				String name = tableInfo.getSourceTable();
				String targetName = tableInfo.getTargetTable();
				String targetSchema = tableInfo.getTargetSchema();
				String columnTypeCheckRequired = tableInfo.getColumnTypeCheckRequired();
				String additionalColumns = tableInfo.getAdditionalColumns();
				if(columnTypeCheckRequired.equals(null)){
					columnTypeCheckRequired=COLUMN_CHECK_DEFAULT;
				}

				if (null == targetName || "".equals(targetName)) {
					targetName = name;
				}
				LOGGER.info("============================================================================");
				LOGGER.info("======READING SOURCE METADATA for TABLE {} : {} OF {} =============", name, ++index,
						totalTableSize);
				LOGGER.info("============================================================================");
			
				ResultSet tbls = md.getTables(null, schema, name, null);
				if(false == tbls.next()){
					nonExistingRdbmsTables.add(name);
					LOGGER.error("Table with name {} in schema {} does not exists in source database", name, schema);
					continue;
				}
				RdbmsTableMetadata tableMD = new RdbmsTableMetadata();
				List<String> pkList = new ArrayList();
				ResultSet primaryKeys = md.getPrimaryKeys(null, schema, name);
				while(primaryKeys.next()){
					String pk = primaryKeys.getString("COLUMN_NAME");
					pkList.add(pk);
				}
				tableMD.setPkList(pkList);
				LOGGER.info("*****Primary key list for table {} is {}", name, pkList);
				
				tableMD.setSchema(schema);
				tableMD.setName(name);
				tableMD.setTargetName(targetName);
				List<Column> colList = new ArrayList<>();
				
				if(compareMetadataFlag.equalsIgnoreCase("FALSE")){
					LOGGER.info("Skipping Datbase Metadata check as columnTypeCheckRequired is set to FALSE");
					String query = new StringBuilder("Select * from ").append(schema).append(".").append(name)
							.append(";").toString();
					LOGGER.info("Executing query for non-type checking flow  :  " +  query);
					PreparedStatement stmt=con.prepareStatement(query);
					ResultSet rs=stmt.executeQuery();
					ResultSetMetaData md1 = rs.getMetaData();
					int i = md1.getColumnCount();
					String columnName;
					for (int j = 1; j <= i; j++)
					{
					   columnName = md1.getColumnLabel(j); 
					   Column col = new Column.ColumnBuilder().name(columnName).type("Default").build();
					   colList.add(col);
					}
					tableMD.setCols(colList);
				}
				
				else{
					LOGGER.info("Processing Datbase Metadata check as columnTypeCheckRequired is set to TRUE");
					ResultSet cols = md.getColumns(null, schema, name, null);
					while (cols.next()) {
						String columnName = cols.getString("COLUMN_NAME");
						String columnType = cols.getString("TYPE_NAME");
						Column col = new Column.ColumnBuilder().name(columnName).type(columnType).build();
						colList.add(col);
					}
					tableMD.setCols(colList);
				}
				tableMD.setAdditionalColumns(additionalColumns);
				tableMD.setColumnTypeCheckRequired(columnTypeCheckRequired);
				AppDataStore.updateRdbmsMetadataCache(tableMD);
			}	
			AppDataStore.updateNonExistingRdbmsTbls(nonExistingRdbmsTables);
			LOGGER.info("Successfully cached rdbms metadata.");
		}catch (SQLException e){
			LOGGER.error("Error while reading rdbms metadata for data source {}, root cause is {}", targetDataSource,
					e.getMessage());
			LOGGER.error(e.getMessage(), e);
		}
		
	
		
	}
	
	

}
