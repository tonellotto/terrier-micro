package it.cnr.isti.hpclab.matching.structures.output;

import java.io.IOException;

import it.cnr.isti.hpclab.matching.structures.SearchRequest;

public class NullResultOutput extends ResultOutput
{
	/** Public constructor */
	public NullResultOutput()
	{
	}
		
	/** {@inheritDoc} */
	@Override
	public void print(final SearchRequest rq) throws IOException 
	{
	}

	/** {@inheritDoc} */
	@Override
	public void close() throws IOException 
	{
	}		
}
