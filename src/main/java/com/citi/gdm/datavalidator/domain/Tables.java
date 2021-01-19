package com.citi.gdm.datavalidator.domain;

import java.util.List;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.commons.lang.builder.ToStringBuilder;

/**
 * 
 * @author ayon.bhattacharya
 *
 */
@XmlRootElement(name="tables")
public class Tables {

	private List<TableInfo> tables;

	public Tables() {
	}

	public Tables(List<TableInfo> tables) {
		super();
		this.tables = tables;
	}

	@XmlElement(name="table")
	public List<TableInfo> getTables() {
		return tables;
	}

	public void setTables(List<TableInfo> tables) {
		this.tables = tables;
	}

	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this);
	}

}
