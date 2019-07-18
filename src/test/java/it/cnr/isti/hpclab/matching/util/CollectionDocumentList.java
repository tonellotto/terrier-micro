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

package it.cnr.isti.hpclab.matching.util;

import java.io.IOException;

import org.terrier.indexing.Collection;
import org.terrier.indexing.Document;

public class CollectionDocumentList implements Collection 
{
	private Document[] docs;
	private int pos;

	public CollectionDocumentList(final Document[] docs) 
	{
		this.docs = docs;
		this.pos = -1;
	}

	@Override
	public boolean endOfCollection() 
	{
		return pos >= docs.length - 1;
	}

	@Override
	public Document getDocument() 
	{
		return docs[pos];
	}

	@Override
	public boolean nextDocument() 
	{
		if (pos < docs.length - 1) {
			pos++;
			return true;
		}
		return false;
	}

	@Override
	public void reset() 
	{
		pos = -1;
	}

	@Override
	public void close() throws IOException 
	{
		// do nothing
	}
}