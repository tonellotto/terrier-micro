package it.cnr.isti.hpclab.matching.structures.query;

import it.cnr.isti.hpclab.MatchingConfiguration;
import it.cnr.isti.hpclab.MatchingConfiguration.Property;

import java.util.Iterator;

import org.apache.log4j.Logger;

public interface QuerySource extends Iterator<String>
{
	public static Logger LOGGER = Logger.getLogger(QuerySource.class);

	@Override
	default public void remove() 
	{
		throw new UnsupportedOperationException();
	}

	public int getQueryId();

	public void reset();

	default public boolean hasIds()
	{
		return MatchingConfiguration.getBoolean(Property.HAS_QUERY_ID);
	}
}