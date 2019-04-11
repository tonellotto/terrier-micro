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
