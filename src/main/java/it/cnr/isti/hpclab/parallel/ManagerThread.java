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

import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Logger;
import org.terrier.structures.Index;
import org.terrier.structures.IndexOnDisk;

import it.cnr.isti.hpclab.MatchingConfiguration;
import it.cnr.isti.hpclab.MatchingConfiguration.Property;
import it.cnr.isti.hpclab.annotations.Managed;
import it.cnr.isti.hpclab.manager.Manager;
import it.cnr.isti.hpclab.matching.MatchingAlgorithm;

public class ManagerThread extends Thread
{
	protected static final Logger LOGGER = Logger.getLogger(ManagerThread.class);
	
	// Private variables
	private final Manager mManager;
	private IndexOnDisk mIndex;

	// Shared variables
	protected final BlockingQueue<SearchRequestMessage> sSearchRequestQueue;
	protected final BlockingQueue<SearchRequestMessage> sResultQueue;
		
	// Static variables
	private static int staticId = 0;
	
	private Manager create_manager() 
	{
		try {
			mIndex = Index.createIndex();
			String matchingAlgorithmClassName =  MatchingConfiguration.get(Property.MATCHING_ALGORITHM_CLASSNAME);
			if (matchingAlgorithmClassName.indexOf('.') == -1)
				matchingAlgorithmClassName = MatchingConfiguration.get(Property.DEFAULT_NAMESPACE) + matchingAlgorithmClassName;
			String mManagerClassName = Class.forName(matchingAlgorithmClassName).asSubclass(MatchingAlgorithm.class).getAnnotation(Managed.class).by();
			
			// return (Manager) (Class.forName(mManagerClassName).asSubclass(Manager.class).getConstructor().newInstance(mIndex));
			return (Manager) Class.forName(mManagerClassName).asSubclass(Manager.class).getConstructor(Index.class).newInstance(mIndex);
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	public ManagerThread(final BlockingQueue<SearchRequestMessage> sreq_queue, final BlockingQueue<SearchRequestMessage> res_queue) 
	{
		super.setName(this.getClass().getSimpleName() + "_" + (++staticId));
		LOGGER.warn(super.getName() + " is going to build is own index copy");	
		
		// shared
		this.sSearchRequestQueue = sreq_queue;
		this.sResultQueue = res_queue;
		// private
		this.mManager = create_manager();
    }
	
	@Override
	public void run() 
	{
		try {
			while (true) {
				SearchRequestMessage m = sSearchRequestQueue.take();
				if (m.isPoison()) // poison pill received, no more queries to process
					break;
			
				LOGGER.info(super.getName() + " processing query " + m.srq.getQueryId() + " : " + m.srq.getOriginalQuery());
			
				m.srq.setResultSet(mManager.run(m.srq));
				
				sResultQueue.put(m);
			}
			// mManager.close();
			mIndex.close();
			// notify I'm done to result writer with a poison pill
			sResultQueue.put(new SearchRequestMessage(null));
		} catch (Exception e) {
			e.printStackTrace();
		}
		LOGGER.info(super.getName() + " terminating...");
	}
}