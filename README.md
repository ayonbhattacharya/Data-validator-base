1. How to generate encrypt property values
==========================================
Extract jasypt-1.9.2-dist.zip (http://www.jasypt.org/download.html ) and move to bin folder.
Run following command to encrypt property value
encrypt.bat input="property value that need to be encrypted" password=Rdbms2hiveValidator algorithm=PBEWithMD5AndDES verbose=false
	Input value is the property value that we want to encrypt like database passwords.
	Password value can be any string but it should match the value of 'jasypt.encryptor.password' property value defined in application.properties file.
e.g., encrypt.bat input="SQLserver2008" password=Rdbms2hiveValidator algorithm=PBEWithMD5AndDES verbose=false
aNht6c4I5nDr8KRvJzusxl2aLwSW+dWp


2. Update application.properties file
=====================================
Change property values according to execution environment.
Configuration files are available in 'conf' folder.

In application.properties file following properties required to be modified.
spring.datasource.servername : source database ip address
spring.datasource.databaseName : source database database name
spring.datasource.username : source database user name
spring.datasource.password : source database encrypted password (refer section-1 for how to encrypt password). If encrypted password is used (which is recommended ) password should be placed inside 'ENC()' otherwise 'ENC()' is not required.

spring.hiveDatasource.servername : target database ip address
spring.hiveDatasource.databaseName : target database database name
spring.hiveDatasource.username : target database user name
spring.hiveDatasource.password : target database user encrypted password (refer section-1 for how to encrypt password). If encrypted password is used (which is recommended ) password should be placed inside 'ENC()' otherwise 'ENC()' is not required.

database.metadata.config.file.location : Source data base tables configuration file location, application will generate report for tables which are configured in this file.

database.data.type.mapping.file.location : data types mapping file location.

database.metadata.reports.folder.location : database meta data reports folder location, Make sure this directory exists before generating report. Also make sure user running this application has write permission to this location.

logging.file : location in which application log file will be created, make sure user running this application has write permission to this location.

app.style.folder.location : Directory location in which custom style sheets are defined for report generation. This folder should have 'css' and 'img' folders with required style sheets and images. Default styles are provided as part of configuration files.

db.table.column.exclude.list : This property defines list of comma separated columns in hive table those will be excluded while comparing the actual table data. This property will be considered only if 'db.compare.data.for.metadata.mismatch' property value is false.
e.g., db.table.column.exclude.list=dbtype,year							

default.row.count.to.validate : Default no of rows that are required to be validated while performing data comparison. This value can be overridden at each table level by configuring 'rowCountToValidate' element value in 'sql-server-metadata-config.xml' file.

db.compare.data.for.metadata.mismatch : If 'db.compare.data.for.metadata.mismatch' value is true then even there is a meta data  mismatch application proceeds with data comparison. Default value is true.
								
client.name : Client name which will be displayed in summary report.

3. Update sql-server-metadata-config.xml
========================================
This file defines the list of tables in sql server for which schema and data validation need to be performed.
	<tables> - root element in which each table configuration needs to define.
	<table> - for each table configuration one table definition is required.
	id - id attribute is used for identification, it do not have any significance in application processing logic as of now.
	<schema> - schema name in which table is present, *mandatory field.
	<name> - table name , *mandatory field
	<rowCountToValidate> - number of rows that need to be validated, optional default value is configured in application.proerties file.

4. Update data-types.properties file (No need to change this file unless we have new mappings)
====================================
This file defined data types mapping between sql server and hive.
This mapping information is used while comparing table meta data between sql server and hive.

5. Create jar file
==================
mvnw clean install or mvn clean install

6. Run application
==================
java -jar <application jar file location> --spring.config.location=file:<application.properties file location> -Xmx2048M -Xss256K -Xmx2048m -XX:MaxPermSize=512m
e.g., java -jar rdbms2hive-validator-0.0.1-SNAPSHOT.jar --spring.config.location=file:/var/conf/application.properties -Xmx2048M -Xss256K -Xmx2048m -XX:MaxPermSize=512m



