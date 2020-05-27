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

import it.cnr.isti.hpclab.MatchingConfiguration.Property;
import it.cnr.isti.hpclab.matching.MatchingAlgorithm;
import it.cnr.isti.hpclab.matching.structures.QueryProperties.RuntimeProperty;
import it.cnr.isti.hpclab.matching.structures.output.ResultOutput;
import it.cnr.isti.hpclab.matching.structures.query.QueryParserException;
import it.cnr.isti.hpclab.matching.structures.query.QuerySource;
import it.cnr.isti.hpclab.matching.structures.query.ThresholdQuerySource;
import it.cnr.isti.hpclab.matching.structures.ResultSet;
import it.cnr.isti.hpclab.matching.structures.SearchRequest;
import it.cnr.isti.hpclab.util.StatsLine;

import java.io.Closeable;
import java.io.IOException;

import org.apache.log4j.Logger;
import org.terrier.structures.Index;
import org.terrier.structures.IndexOnDisk;

import it.cnr.isti.hpclab.annotations.Managed;
import it.cnr.isti.hpclab.manager.Manager;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * This class implements the "entry point" of a batch experiment for query processing.
 * It is responsible to parse the file containing the query stream, to process every query
 * in a Java object, to manage the results cache for already-processed queries, and to 
 * invoke the Manager component, responsible for driving the processing of every single query.
 * Eventually, it collects the results produced for every query and post-process them, for
 * correctness tests and/or for effectiveness evaluation.
 */
public class Querying implements Closeable
{	
	private static final Logger LOGGER = Logger.getLogger(Querying.class);	
	
	/** The number of matched queries. */
	protected int mMatchingQueryCount = 0;

	/** Data structures */
	protected IndexOnDisk  mIndex;
	protected QuerySource  mQuerySource;
	protected Manager      mManager;
	protected ResultOutput mResultOutput;
	
	public Querying() 
	{
		mIndex = createIndex();
		mManager = createManager(mIndex);
		
		mQuerySource = createQuerySource();
		mResultOutput = createResultOutput();
	}
	
	public static IndexOnDisk createIndex()
	{
		return Index.createIndex();
	}
	
	public static Manager createManager(final Index index) 
	{
		try {
			String matchingAlgorithmClassName =  MatchingConfiguration.get(Property.MATCHING_ALGORITHM_CLASSNAME);
			if (matchingAlgorithmClassName.indexOf('.') == -1)
				matchingAlgorithmClassName = MatchingConfiguration.get(Property.DEFAULT_NAMESPACE) + matchingAlgorithmClassName;
			String mManagerClassName = Class.forName(matchingAlgorithmClassName).asSubclass(MatchingAlgorithm.class).getAnnotation(Managed.class).by();
			 
			return (Manager) Class.forName(mManagerClassName).asSubclass(Manager.class).getConstructor(Index.class).newInstance(index);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public static QuerySource createQuerySource() 
	{
		try {
			String querySourceClassName =  MatchingConfiguration.get(Property.QUERY_SOURCE_CLASSNAME);
			if (querySourceClassName.indexOf('.') == -1)
				querySourceClassName = MatchingConfiguration.get(Property.DEFAULT_NAMESPACE) + querySourceClassName;
			return (QuerySource) (Class.forName(querySourceClassName).asSubclass(QuerySource.class).getConstructor().newInstance());
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	public static ResultOutput createResultOutput()
	{
		return ResultOutput.newInstance(MatchingConfiguration.get(Property.RESULTS_OUTPUT_TYPE));
	}

	@Override
	public void close() throws IOException 
	{
		// mManager.close();
		mIndex.close();
		mResultOutput.close();
	}

	public void processQueries() throws IOException, QueryParserException 
	{
		mMatchingQueryCount = 0;
		mQuerySource.reset();

		final long startTime = System.currentTimeMillis();

		boolean doneSomeQueries = false;
		
		// iterating through the queries
		while (mQuerySource.hasNext()) {
			String query = mQuerySource.next();
			int qid   = mQuerySource.getQueryId();
			
			float  qth   = 0.0f;
			if (mQuerySource instanceof ThresholdQuerySource)
				qth = ((ThresholdQuerySource) mQuerySource).getQueryThreshold();
			
			// process the query
			long processingStartTime = System.currentTimeMillis();
			processQuery(qid, query, qth);
			long processingEndTime = System.currentTimeMillis();
			if (LOGGER.isInfoEnabled())
				LOGGER.info("Time to process query: " + ((processingEndTime - processingStartTime) / 1000.0D));
			doneSomeQueries = true;
		}
		if (doneSomeQueries)
			LOGGER.info("Finished topics, executed " + mMatchingQueryCount +
						" queries in " + ((System.currentTimeMillis() - startTime) / 1000.0d) +
						" seconds");
	}

	public SearchRequest processQuery(final int queryId, final String query, final float threshold) throws IOException, QueryParserException
	{
		checkNotNull(queryId);
		checkNotNull(query);
		
		if (LOGGER.isInfoEnabled())
			LOGGER.info(queryId + " : " + query);
		
		SearchRequest srq = new SearchRequest(queryId, query);
		srq.getQuery().addMetadata(RuntimeProperty.INITIAL_THRESHOLD, Float.toString(threshold));
		
		if (LOGGER.isInfoEnabled())
			LOGGER.info("Processing query: " + queryId + ": '" + query + "'");
		mMatchingQueryCount++;
		ResultSet rs = mManager.run(srq);
		srq.setResultSet(rs); //TODO: shouldn't this be inside runMatching?
		PrintStats(srq);
		if (rs.size() != 0)
			mResultOutput.print(srq);
		return srq;
	}

	private static void PrintStats(SearchRequest srq) 
	{
		StatsLine statsLine = new StatsLine();
		
		statsLine.add("type", "\"query\"");
		statsLine.add("qid", Integer.toString(srq.getQueryId()));
		
		for (RuntimeProperty prop: RuntimeProperty.values())
			if (srq.getQuery().getMetadata(prop) != null)
				statsLine.add(prop.toString(), srq.getQuery().getMetadata(prop));

		statsLine.print();
	}
}
