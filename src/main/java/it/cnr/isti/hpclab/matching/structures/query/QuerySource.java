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