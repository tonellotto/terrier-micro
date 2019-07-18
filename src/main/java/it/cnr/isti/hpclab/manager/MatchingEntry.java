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

package it.cnr.isti.hpclab.manager;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.terrier.structures.LexiconEntry;
import org.terrier.structures.postings.IterablePosting;

import it.cnr.isti.hpclab.maxscore.structures.BlockEnumerator;

public class MatchingEntry
{
	public static final Comparator<MatchingEntry> SORT_BY_DOCID    = (o1, o2) -> {return Integer.compare(o1.posting.getId(), o2.posting.getId()); };
	public static final Comparator<MatchingEntry> SORT_BY_MAXSCORE = (o1, o2) -> {return Float.compare(o1.maxscore, o2.maxscore); };	

	// These members are public to avoid getter/setter boilerplate
	public final String term;
	public final LexiconEntry entry;
	public final float maxscore;
	public final BlockEnumerator blockEnum;
	
	public IterablePosting posting;
	
	public MatchingEntry(final String term, final IterablePosting posting, final LexiconEntry entry)
	{
		this(term, posting, entry, Float.MAX_VALUE, null);
	}

	public MatchingEntry(final String term, final IterablePosting posting, final LexiconEntry entry, final float maxscore)
	{
		this(term, posting, entry, maxscore, null);
	}

	public MatchingEntry(final String term, final IterablePosting posting, final LexiconEntry entry, final float maxscore, final BlockEnumerator blockEnum)
	{
		this.term = term;
		this.posting = posting;
		this.entry = entry;
		this.maxscore = maxscore;
		this.blockEnum = blockEnum;
	}
	
	@Override
	public String toString()
	{
		return posting.toString() + ", [" + entry.getDocumentFrequency() + "," + entry.getFrequency() + "] <" + maxscore + ">"  + " {" + blockEnum + "}"; 
	}

	public static void sortByCurrentDocid(List<MatchingEntry> list)
	{
		Collections.sort(list, SORT_BY_DOCID);
	}

	public static void sortByMaxScore(List<MatchingEntry> list)
	{
		Collections.sort(list, SORT_BY_MAXSCORE);
	}
}