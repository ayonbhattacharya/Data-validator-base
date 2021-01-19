package com.citi.gdm.datavalidator.configuration;

import java.util.HashMap;

import javax.sql.DataSource;

import org.apache.velocity.app.VelocityEngine;
import org.apache.velocity.runtime.VelocimacroFactory;
import org.springframework.boot.autoconfigure.jdbc.DataSourceBuilder;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.oxm.jaxb.Jaxb2Marshaller;
import org.springframework.ui.velocity.VelocityEngineFactory;

import com.citi.gdm.datavalidator.domain.TableInfo;
import com.citi.gdm.datavalidator.domain.Tables;

/**
 * 
 * @author ayon.bhattacharya
 *
 */
@Configuration
public class AppConfig {

	//---Constructor
	public AppConfig(){
		
	}
	
	@Bean
	public Jaxb2Marshaller jaxb2Marshaller(){
		Jaxb2Marshaller jaxb2Marshaller = new Jaxb2Marshaller();
		jaxb2Marshaller.setClassesToBeBound(new Class[]{Tables.class,TableInfo.class});
		jaxb2Marshaller.setMarshallerProperties(new HashMap<String, Object>() {
			private static final long serialVersionUID = 1L;

			{
				put("jaxb.formatted.output", Boolean.TRUE);
			}
		});
		return jaxb2Marshaller;
	}
	
	
	@Bean
	@Primary
	@ConfigurationProperties(prefix = "spring.sourceDatabase")
	public DataSource primaryDataSource(){
		return DataSourceBuilder.create().build();
	}
	
	@Bean
	@ConfigurationProperties(prefix = "spring.targetDatabase")
	public DataSource hawqDataSource() {
		return DataSourceBuilder.create().build();
	}
	
	@Bean
	public VelocityEngineFactory velocityEngine(){
		VelocityEngineFactory velocityEngineFactory = new VelocityEngineFactory();
		velocityEngineFactory.setResourceLoaderPath("classpath:/templates/");
		return velocityEngineFactory;
		
	}
}
