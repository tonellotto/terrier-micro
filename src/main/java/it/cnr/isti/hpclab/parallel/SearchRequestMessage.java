package it.cnr.isti.hpclab.parallel;

import it.cnr.isti.hpclab.matching.structures.SearchRequest;

public class SearchRequestMessage 
{
	// If srq is null, this message is a "poison pill", and the thread receiving it must terminate.
	public final SearchRequest srq;
	
	public SearchRequestMessage(final SearchRequest srq)
	{
		this.srq = srq;
	}
	
	public boolean isPoison()
	{
		return this.srq == null;
	}
}
