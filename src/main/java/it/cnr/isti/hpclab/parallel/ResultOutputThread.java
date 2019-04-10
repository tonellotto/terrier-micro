package it.cnr.isti.hpclab.parallel;

import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Logger;

import it.cnr.isti.hpclab.MatchingConfiguration;
import it.cnr.isti.hpclab.MatchingConfiguration.Property;
import it.cnr.isti.hpclab.matching.structures.SearchRequest;
import it.cnr.isti.hpclab.matching.structures.Query.RuntimeProperty;
import it.cnr.isti.hpclab.matching.structures.output.ResultOutput;
import it.cnr.isti.hpclab.util.StatsLine;

public class ResultOutputThread extends Thread
{
	protected static final Logger LOGGER = Logger.getLogger(ResultOutputThread.class);
	
	// shared
	private final BlockingQueue<SearchRequestMessage> sResultQueue;
	
	// private
	private final ResultOutput mResultOutput;
	private int mPoisonPills;
	
	public ResultOutputThread(final BlockingQueue<SearchRequestMessage> res_queue, final int numThreads) 
	{
		super.setName(this.getClass().getSimpleName());
		LOGGER.warn(super.getName() + " is going to build is own index copy");	
		
		// shared
		this.sResultQueue = res_queue;
		// private
		this.mResultOutput = ResultOutput.newInstance(MatchingConfiguration.get(Property.RESULTS_OUTPUT_TYPE));
		this.mPoisonPills = numThreads;
    }

	@Override
	public void run() 
	{
		try {
			while (true) {
				SearchRequestMessage m = sResultQueue.take();
				if (m.isPoison()) {
					LOGGER.info("Received poison pill");
					if (--mPoisonPills == 0)
						break;
					else
						continue;
				}
				if (m.srq.getResultSet().size() != 0) {
					printStats(m.srq);
					mResultOutput.print(m.srq);
				}
			}
			mResultOutput.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		LOGGER.info(super.getName() + " terminating...");
	}

	private static void printStats(SearchRequest srq) 
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
