package com.citi.gdm.datavalidator.util;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

import com.citi.gdm.datavalidator.data.store.AppDataStore;
import com.citi.gdm.datavalidator.domain.RdbmsTableMetadata;
import com.citi.gdm.datavalidator.domain.TableInfo;

/**
 * 
 * @author ayon.bhattacharya
 *
 */
public class Utils {

	private Utils(){}
	
	public static String getFormattedDateTimeOffset(String _x) {
		if (_x.length() < 29) {
			String[] split = _x.split("\\s");
			if (split.length == 3) {
				String time = null;
				int length = split[1].length();
				if (length == 11) {
					time = split[1];
				} else if (length < 11) {
					if (length == 8) {
						split[1] = split[1] + ".";
					}
					time = StringUtils.rightPad(split[1], 11, '0');
				} else {
					time = StringUtils.substring(split[1], 0, 11);
				}
				_x = new StringBuilder().append(split[0]).append(" ").append(time).append(" ").append(split[2])
						.toString();
			}

		}
		return _x;
	}
	
	public static String getSourceSchemaNameForReport(List<RdbmsTableMetadata> rdbmsTableInfoList) {
		Set<String> schemaNameList = new HashSet<>();
		for (RdbmsTableMetadata rdbmsTableMetadata : rdbmsTableInfoList) {
			schemaNameList.add(rdbmsTableMetadata.getSchema());
		}
		return schemaNameList.toString();
	}
	
	public static String getSchemaName(String sourceTblName) {

		List<TableInfo> configTableInfoList = AppDataStore.getConfigTableInfoList();
		for (TableInfo tableInfo : configTableInfoList) {
			String name = tableInfo.getSourceTable();
			if (sourceTblName.equalsIgnoreCase(name)) {
				return tableInfo.getSourceTable();
			}
		}
		return configTableInfoList.size() > 0 ? configTableInfoList.get(0).getSourceSchema() : "N/A";
	}
	
	public static String getTargetSchemaName(String targetTblName) {

		List<TableInfo> configTableInfoList = AppDataStore.getConfigTableInfoList();
		for (TableInfo tableInfo : configTableInfoList) {
			String name = tableInfo.getTargetTable();
			if (targetTblName.equalsIgnoreCase(name)) {
				return tableInfo.getSourceTable();
			}
		}
		return configTableInfoList.size() > 0 ? configTableInfoList.get(0).getSourceSchema() : "N/A";
	}
}
