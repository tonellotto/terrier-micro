package it.cnr.isti.hpclab.matching;

import it.cnr.isti.hpclab.manager.Manager;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.terrier.structures.postings.IterablePosting;

public interface MatchingAlgorithm
{
	static final Logger LOGGER = Logger.getLogger(MatchingAlgorithm.class);

	void setup(final Manager manager);
	
	default long match() throws IOException
	{
		return this.match(0, IterablePosting.END_OF_LIST);
	}

	default long match(final int from) throws IOException
	{
		return this.match(from, IterablePosting.END_OF_LIST);
	}

	// Take care to correctly sort the manager's enums
	long match(final int from, final int to) throws IOException;
}
