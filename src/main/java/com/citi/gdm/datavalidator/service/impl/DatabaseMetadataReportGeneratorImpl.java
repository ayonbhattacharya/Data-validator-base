package com.citi.gdm.datavalidator.service.impl;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.velocity.app.VelocityEngine;
import org.apache.velocity.tools.generic.MathTool;
import org.apache.velocity.tools.generic.NumberTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.ui.velocity.VelocityEngineUtils;

import com.citi.gdm.datavalidator.data.store.AppDataStore;
import com.citi.gdm.datavalidator.domain.DatabaseMetadataSummaryReport;
import com.citi.gdm.datavalidator.service.DatabaseMetadataReportGenerator;

/**
 * @author ayon.bhattacharya
 *
 */
@Service
public class DatabaseMetadataReportGeneratorImpl implements DatabaseMetadataReportGenerator {

	private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseMetadataReportGeneratorImpl.class);

	@Value("${database.metadata.reports.folder.location}")
	private String databaseMetadataReportsFolderLocation;

	@Autowired
	private VelocityEngine velocityEngine;

	@Value("${app.style.folder.location}")
	private String styleDir;

	@Value("${client.name}")
	private String clientName;

	/**
	 *
	 */
	public DatabaseMetadataReportGeneratorImpl() {
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see com.emc.ps.appmod.service.DatabaseMetadataReportGenerator#
	 * generateMetadataReport(com.emc.ps.appmod.domain.
	 * DatabaseMetadataSummaryReport)
	 */
	@Override
	public void generateMetadataReport(DatabaseMetadataSummaryReport report) {
		LOGGER.info("Started generating metadata report.");
		Map<String, Object> model = new HashMap<>();
		model.put("clientName", clientName);
		model.put("report", report);
		model.put("dbSystem", report.getDbSystem());
		model.put("numberTool", new NumberTool());
		model.put("mathTool", new MathTool());

		String mdRepDirPath = AppDataStore.getReportFolder();
		System.out.println("Deb1  " + mdRepDirPath);
		if (StringUtils.isBlank(mdRepDirPath)) {
			databaseMetadataReportsFolderLocation = databaseMetadataReportsFolderLocation.endsWith(File.separator)
					? databaseMetadataReportsFolderLocation : databaseMetadataReportsFolderLocation + File.separator;
			mdRepDirPath = new StringBuilder().append(databaseMetadataReportsFolderLocation)
					.append(new SimpleDateFormat("dd-MM-yyyy_HH-mm-ss").format(new Date())).toString();
			AppDataStore.setReportFolder(mdRepDirPath);
		}
		System.out.println("Deb2  " + mdRepDirPath);
		File mdRepDir = new File(mdRepDirPath);
		if (false == mdRepDir.exists()) {
			mdRepDir.mkdir();
		}
		// Download static files
		// css and image files
		downloadStaticFiles(mdRepDir.getPath());

		String mdRepFileName = new StringBuilder().append(mdRepDir.getPath()).append(File.separator)
				.append("Summary.html").toString();
		Writer sw = new StringWriter();
		VelocityEngineUtils.mergeTemplate(velocityEngine, "metadata-summary.vm", "UTF-8", model, sw);
		try (FileWriter fw = new FileWriter(mdRepFileName);) {
			String reportContent = sw.toString();
			LOGGER.debug("Creating report file {} with content {}.", mdRepFileName, reportContent);
			fw.write(reportContent);
		} catch (IOException e) {
			LOGGER.error("Error while creating summary report, root cause is {}", e.getMessage());
			LOGGER.error(e.getMessage(), e);
			// do not throw exception , log and continue for next report
			//throw new DataValidatorException(e.getMessage(), e);
		}
		LOGGER.info("Ended generating metadata report.");
	}

	private void downloadStaticFiles(String targetDir) {
		File f = new File(styleDir);
		// check if style dir exists
		if (f.exists() && f.isDirectory()) {
			// download css folder
			String _css = "css";
			download(targetDir, f, _css);

			// download image folder
			String _img = "img";
			download(targetDir, f, _img);
		}
	}

	private void download(String targetDir, File f, String _fol) {
		File imgFolder = new File(
				new StringBuilder().append(f.getPath()).append(File.separator).append(_fol).toString());
		// check if dir exists
		if (imgFolder.exists() && imgFolder.isDirectory()) {
			File[] imgFiles = imgFolder.listFiles();
			for (File imgFile : imgFiles) {
				if (imgFile.isFile()) {
					downloadFile(imgFile, targetDir, _fol);
				}
			}
		}
	}

	private void downloadFile(File source, String targetDir, String subDir) {
		String fileName = source.getName();
		String outputFolder = new StringBuilder().append(targetDir).append(File.separator).append(subDir)
				.append(File.separator).toString();
		if (false == new File(outputFolder).exists()) {
			new File(outputFolder).mkdir();
		}
		String outputFile = new StringBuilder().append(outputFolder).append(fileName).toString();
		try (BufferedInputStream input = new BufferedInputStream(new FileInputStream(source));
				BufferedOutputStream output = new BufferedOutputStream(new FileOutputStream(outputFile));) {
			IOUtils.copy(input, output);
		} catch (IOException e) {
			LOGGER.error("Error while downloading style sheets, root cause is {}", e.getMessage());
			LOGGER.error(e.getMessage(), e);
		}
	}

}