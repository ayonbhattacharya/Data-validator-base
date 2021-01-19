package com.citi.gdm.datavalidator.domain;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/**
 * 
 * @author ayon.bhattacharya
 *
 */

	@XmlRootElement(name = "table", namespace="tables")
	@XmlType(propOrder = { "sourceSchema","sourceTable","targetSchema","targetTable","additionalColumns","rowCountToValidate","columnTypeCheckRequired"})
	@Service
	public class TableInfo {

		/**
		 *
		 */
		public TableInfo() {
		}

		private String sourceTable;
		private String sourceSchema;
		private String targetTable;
		private String targetSchema;
		private String additionalColumns;
		private String rowCountToValidate;
		private String columnTypeCheckRequired;
		
		@XmlElement(name = "sourceSchema")
		public String getSourceSchema() {
			return sourceSchema;
		}
		public void setSourceSchema(String sourceSchema) {
			this.sourceSchema = sourceSchema;
		}
		
		@XmlElement(name = "sourceTable")
		public String getSourceTable() {
			return sourceTable;
		}
		public void setSourceTable(String sourceTable) {
			this.sourceTable = sourceTable;
		}
		
		@XmlElement(name = "targetTable")
		public String getTargetTable() {
			return targetTable;
		}
		public void setTargetTable(String targetTable) {
			this.targetTable = targetTable;
		}
		@XmlElement(name = "targetSchema")
		public String getTargetSchema() {
			return targetSchema;
		}
		public void setTargetSchema(String targetSchema) {
			this.targetSchema = targetSchema;
		}
		@XmlElement(name = "additionalColumns")
		public String getAdditionalColumns() {
			return additionalColumns;
		}
		public void setAdditionalColumns(String additionalColumns) {
			this.additionalColumns = additionalColumns;
		}
		@XmlElement(name = "rowCountToValidate")
		public String getRowCountToValidate() {
			return rowCountToValidate;
		}
		public void setRowCountToValidate(String rowCountToValidate) {
			this.rowCountToValidate = rowCountToValidate;
		}
		@XmlElement(name = "columnTypeCheckRequired")
		public String getColumnTypeCheckRequired() {
			return columnTypeCheckRequired;
		}
		public void setColumnTypeCheckRequired(String columnTypeCheckRequired) {
			this.columnTypeCheckRequired = columnTypeCheckRequired;
		}



}

