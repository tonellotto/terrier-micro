package it.cnr.isti.hpclab.manager;

import java.util.Comparator;

import org.terrier.structures.LexiconEntry;
import org.terrier.structures.postings.IterablePosting;

import it.cnr.isti.hpclab.maxscore.structures.BlockEnumerator;

public class Tuple implements Comparable<Tuple>
{
	public static final Comparator<Tuple> SORT_BY_DOCID = (o1, o2) -> {return Integer.compare(o1.posting.getId(), o2.posting.getId()); };
	public static final Comparator<Tuple> SORT_BY_SCORE = (o1, o2) -> {return Float.compare(o1.maxscore, o2.maxscore); };	

	public final String term;
	public 		 IterablePosting posting;
	public final LexiconEntry entry;
	public final float maxscore;
	public final BlockEnumerator blockEnum;
	
	public final Comparator<Tuple> comp;

	public Tuple(final String term, final IterablePosting posting, final LexiconEntry entry, final float maxscore, final Comparator<Tuple> comp)
	{
		this.term = term;
		this.posting = posting;
		this.entry = entry;
		this.maxscore = maxscore;
		this.blockEnum = null;
		
		this.comp = comp;
	}

	
	public Tuple(final String term, final IterablePosting posting, final LexiconEntry entry, final float maxscore, final BlockEnumerator blockEnum, final Comparator<Tuple> comp)
	{
		this.term = term;
		this.posting = posting;
		this.entry = entry;
		this.maxscore = maxscore;
		this.blockEnum = blockEnum;
		
		this.comp = comp;
	}
	
	@Override
	public String toString()
	{
		return posting.toString() + ", [" + entry.getDocumentFrequency() + "," + entry.getFrequency() + "] <" + maxscore + ">"  + " {" + blockEnum + "}"; 
	}

	@Override
	public int compareTo(Tuple that) 
	{
		return comp.compare(this, that);
	}		
}