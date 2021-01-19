package com.citi.gdm.datavalidator.service.impl;


import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.jdbc.DataSourceBuilder;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.stereotype.Service;

import com.citi.gdm.datavalidator.data.store.AppDataStore;
import com.citi.gdm.datavalidator.domain.Column;
import com.citi.gdm.datavalidator.domain.RdbmsTableMetadata;
import com.citi.gdm.datavalidator.domain.TableInfo;
import com.citi.gdm.datavalidator.service.DatabaseMetadataReader;
@Service
public class DatabaseMetadataReaderImpl implements DatabaseMetadataReader{
	private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseMetadataReaderImpl.class);
	private static final String COLUMN_CHECK_DEFAULT = "TRUE";
	
	@Autowired
	private DataSource datasource;
	//private DataSource datasource = dataSource();
	
	@Value("${db.compare.metadata.values}")
	private String compareMetadataFlag;
	
	@Override
	public void processDatabaseMetadata() {
		LOGGER.info("Started processing database Metadata");
		try {
			datasource.setLoginTimeout(30);
		}catch(Exception e){
			LOGGER.debug("Login Timeout Exception");
			e.printStackTrace();
		}
		
		Connection con = DataSourceUtils.getConnection(datasource);
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
			LOGGER.error("Error while reading rdbms metadata for data source {}, root cause is {}", datasource,
					e.getMessage());
			LOGGER.error(e.getMessage(), e);
		}
		
	
		
	}
	
	
	@Value("${spring.sourceDatabase.url}")
	private String url;
	
	@Value("${spring.sourceDatabase.username}")
	private String username;
	@Value("${spring.sourceDatabase.password}")
	private String password;
	@Value("${spring.sourceDatabase.driver-class-name}")
	private String driverName;
	@Value("${spring.sourceDatabase.maxActive}")
	private String maxActive;
	@Value("${spring.sourceDatabase.minIdle}")
	private String minIdle;
	@Value("${spring.sourceDatabase.maxIdle}")
	private String maxIdle;
	@Value("${spring.sourceDatabase.maxWait}")
	private String maxWait;
	
	
	public DataSource dataSource(){
		LOGGER.info("Teradata password is" + password);
		return DataSourceBuilder.create().url(url).username(username).password(password)
				.driverClassName(driverName)
				.build();
	}
	
	

}

