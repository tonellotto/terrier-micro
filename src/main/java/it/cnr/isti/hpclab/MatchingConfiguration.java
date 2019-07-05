/*
 * Micro query processing framework for Terrier 5
 *
 * Copyright (C) 2018-2019 Nicola Tonellotto 
 *
 *  This library is free software; you can redistribute it and/or modify it
 *  under the terms of the GNU Lesser General Public License as published by the Free
 *  Software Foundation; either version 3 of the License, or (at your option)
 *  any later version.
 *
 *  This library is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 *  or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 *  for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses/>.
 *
 */

package it.cnr.isti.hpclab;

import java.io.File;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.SystemConfiguration;

import org.apache.log4j.Logger;

import it.cnr.isti.hpclab.matching.structures.model.BM25;
import it.cnr.isti.hpclab.matching.RankedOr;
import it.cnr.isti.hpclab.manager.Manager;
import it.cnr.isti.hpclab.matching.structures.query.TextQuerySource;

public class MatchingConfiguration 
{
	public static Logger LOGGER = Logger.getLogger(MatchingConfiguration.class);
	
	public enum Property 
	{
		DEFAULT_NAMESPACE			("micro.namespace",    		"it.cnr.isti.hpclab.matching."),

		INDEX_PATH					("micro.index.path",   		"."),
		INDEX_PREFIX				("micro.index.prefix", 		"data"),

		QUERY_SOURCE_CLASSNAME		("micro.query.source",		TextQuerySource.class.getName()),
		MANAGER_CLASSNAME			("micro.manager", 	   		Manager.class.getName()),
		WEIGHTING_MODEL_CLASSNAME	("micro.model",		   		BM25.class.getName()),
		MATCHING_ALGORITHM_CLASSNAME("micro.matching",	   		RankedOr.class.getName()),

		QUERY_FILE		 			("micro.queries",      	   	""),
		HAS_QUERY_ID				("micro.queries.id",   	    "false"),
		MUST_TOKENISE				("micro.queries.tokenise",  "true"),
		NUM_QUERIES					("micro.queries.num",		"all"),
		LOWERCASE					("micro.queries.lowercase", "true"),
		TERM_PIPELINE				("micro.termpipelines",		"Stopwords,PorterStemmer"),

		IGNORE_LOW_IDF_TERMS		("micro.ignore.low.idf", 	"true"),
		TOP_K						("micro.topk",				"1000"),

		RESULTS_OUTPUT_TYPE			("micro.results.output.type",	"null"),
	 	RESULTS_FILENAME          	("micro.results.filename",		"results.gz"),
		
	 	THRESHOLD_FILE     			("micro.queries.threshold",	""),

	 	NULL						("null", "null");
		  
		private final String mName;
		private final String mDefaultValue;
		
		private Property(final String name, final String defaultValue)
		{
			mName = name;
			mDefaultValue = defaultValue;	
		}
		
		@Override
		public String toString()
		{
			return mName;
		}
		
		public String getDefaultValue()
		{
			return mDefaultValue;
		}
		
	}
	
	protected static CompositeConfiguration config = null;
	
	protected MatchingConfiguration() 
	{
	}

	public static Configuration getConfiguration() 
	{
		if (config == null) { 	
			config = new CompositeConfiguration();
			config.setDelimiterParsingDisabled(true);
			((CompositeConfiguration)config).addConfiguration(new SystemConfiguration());
			try {
				((CompositeConfiguration)config).addConfiguration(new PropertiesConfiguration(System.getProperty("user.dir") + File.separator + "micro.properties"));
				// LOGGER.info("\"micro.properties\" file found in current directory " + Paths.get("").toAbsolutePath());
			} catch (ConfigurationException e) {
				LOGGER.warn("\"micro.properties\" file not found in current directory, trying to load it from classpath");
				try {
					((CompositeConfiguration)config).addConfiguration(new PropertiesConfiguration(MatchingConfiguration.class.getResource("/micro.properties")));
					LOGGER.info("\"micro.properties\" file found in classpath");
				} catch (ConfigurationException e1) {
					LOGGER.warn("\"micro.properties\" file not found, using system (e.g. -D...=...) properties and default values");
				}
			}
		}
		return config;
	}
	
	public static String get(Property p)
	{
		if (config == null)
			getConfiguration();
		return config.getString(p.toString(), p.getDefaultValue());
	}

	public static void set(Property p, String v)
	{
		if (config == null)
			getConfiguration();
		config.setProperty(p.toString(), (v == null ? p.getDefaultValue() : v));
	}

	public static boolean getBoolean(Property p)
	{
		if (config == null)
			getConfiguration();
		return config.getBoolean(p.toString(), Boolean.parseBoolean(p.getDefaultValue()));
	}

	public static int getInt(Property p)
	{
		if (config == null)
			getConfiguration();
		return config.getInt(p.toString(), Integer.parseInt(p.getDefaultValue()));
	}
	
	public static double getDouble(Property p)
	{
		if (config == null)
			getConfiguration();
		return config.getDouble(p.toString(), Double.parseDouble(p.getDefaultValue()));
	}

}
