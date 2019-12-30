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

package it.cnr.isti.hpclab.parallel;

import it.cnr.isti.hpclab.MatchingConfiguration;
import it.cnr.isti.hpclab.MatchingConfiguration.Property;
import it.cnr.isti.hpclab.matching.structures.query.QueryParserException;
import it.cnr.isti.hpclab.matching.structures.query.QuerySource;
import it.cnr.isti.hpclab.matching.structures.query.ThresholdQuerySource;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import it.cnr.isti.hpclab.matching.structures.SearchRequest;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Logger;

public class ParallelQuerying
{	
	private static final Logger LOGGER = Logger.getLogger(ParallelQuerying.class);	
	
	/** The number of matched queries. */
	protected int mMatchingQueryCount = 0;

	/** Data structures */
	private QuerySource  mQuerySource;

	// to be shared
	protected BlockingQueue<SearchRequestMessage> sSearchRequestQueue;
	protected BlockingQueue<SearchRequestMessage> sResultQueue;
	
	// private
	private final int mNumThreads;
	private ObjectList<Thread> mThreads;
	
	public ParallelQuerying() 
	{	
		mQuerySource = createQuerySource();

		sSearchRequestQueue = new ArrayBlockingQueue<SearchRequestMessage>(1 << 10);
		sResultQueue = new ArrayBlockingQueue<SearchRequestMessage>(1 << 10);

		mNumThreads = Runtime.getRuntime().availableProcessors();
		
		mThreads = new ObjectArrayList<Thread>(mNumThreads + 1);
		
		Thread th;
		for (int i = 0; i < mNumThreads; i++) {
			th = new ManagerThread(sSearchRequestQueue, sResultQueue);
			mThreads.add(th);
			th.start();
			// (new QueryProcessThread(sSearchRequestQueue, sResultQueue)).start();
		}

		th = new ResultOutputThread(sResultQueue, mNumThreads);
		mThreads.add(th);
		th.start();
		// (new ResultOutputThread(sResultQueue, mNumThreads)).start();
	}
	
	private static QuerySource createQuerySource() 
	{
		try {
			String querySourceClassName =  MatchingConfiguration.get(Property.QUERY_SOURCE_CLASSNAME);
			if (querySourceClassName.indexOf('.') == -1)
				querySourceClassName = MatchingConfiguration.get(Property.DEFAULT_NAMESPACE) + querySourceClassName;
			return (QuerySource) (Class.forName(querySourceClassName).asSubclass(QuerySource.class).getConstructor().newInstance());
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public void processQueries() throws IOException, QueryParserException 
	{
		mQuerySource.reset();

		final long startTime = System.currentTimeMillis();

		// iterating through the queries
		while (mQuerySource.hasNext()) {
			String query = mQuerySource.next();
			int qid   = mQuerySource.getQueryId();
			
			float  qth   = 0.0f;
			if (mQuerySource instanceof ThresholdQuerySource)
				qth = ((ThresholdQuerySource) mQuerySource).getQueryThreshold();
			
			processQuery(qid, query, qth);
		}
		// notify processors that queries are over with a poison pill per processor
		try {
			for (int i = 0; i < mNumThreads; ++i) {
				sSearchRequestQueue.put(new SearchRequestMessage(null));
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		mThreads.forEach(t -> {	try { t.join();	} catch (InterruptedException e) { e.printStackTrace(); } } );

		LOGGER.info("Finished topics, executed " + mMatchingQueryCount +
					" queries in " + ((System.currentTimeMillis() - startTime) / 1000.0d) +
					" seconds");
	}

	public void processQuery(final int queryId, final String query, final float threshold) throws IOException, QueryParserException
	{
		try {
			sSearchRequestQueue.put(new SearchRequestMessage(new SearchRequest(queryId, query)));
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
