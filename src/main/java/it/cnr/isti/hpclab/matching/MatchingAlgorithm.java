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
