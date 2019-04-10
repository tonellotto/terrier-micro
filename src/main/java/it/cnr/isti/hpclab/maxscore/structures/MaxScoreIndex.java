package it.cnr.isti.hpclab.maxscore.structures;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.apache.log4j.Logger;
import org.terrier.structures.Index;
import org.terrier.structures.IndexOnDisk;
import org.terrier.utility.ApplicationSetup;
import org.terrier.utility.Files;
import org.terrier.utility.TerrierTimer;
import org.terrier.utility.io.RandomDataInput;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

/** 
 * This class represents an additional data structure to a regular Terrier index.
 * It is basically a disk-based array of float values, one per lexicon term, storing
 * the maximum score contribution computed a-priori, given a weigthing model.
 * 
 * It simply provides, for a termid, the associated maxscore value.
 * 
 * If the property "preload.maxscore.index" is set to "true", the whole array of values is
 * preloaded in main memory when the index is created. Since it can occupy a lot of memory,
 * a simple LRU cache manages not-preloaded max-scores. Its size is configurable.
 * 
 * To avoid changes to properties, this structure is managed externally from Terrier, and must be explicitely created.
 */
public class MaxScoreIndex implements Closeable
{
	/** The number of entries in the file.*/
	private int numberOfEntries;
	
	private float maxScores[] = null;
	/** actual underlying data file */
	private RandomDataInput dataFile = null;
	
	private LoadingCache<Integer, Float> msCache = null;
	private static int CACHE_SIZE = 10 * 1024;
	
	public static final String USUAL_EXTENSION = ".ms";
		
	protected static final Logger logger = Logger.getLogger(MaxScoreIndex.class);
	
	public MaxScoreIndex(final Index index) throws IOException
	{
		this( ((IndexOnDisk)index).getPath() + File.separator + ((IndexOnDisk)index).getPrefix() + MaxScoreIndex.USUAL_EXTENSION);
	}
		
	public MaxScoreIndex(final String filename) throws IOException
    {
		this.dataFile = Files.openFileRandom(filename);
        this.numberOfEntries = (int) (dataFile.length() / (long)(Float.SIZE >> 3));
        initialise();
    }

	protected void initialise() throws IOException
	{
		if (ApplicationSetup.getProperty("preload.maxscore.index", "false").equals("true")) {
			logger.info("Loading max score values for maxscore structure into memory");
			maxScores = new float[numberOfEntries];
		
			TerrierTimer tt = new TerrierTimer("Max Score Index loading", numberOfEntries);
			tt.start();		
			for (int i = 0; i < numberOfEntries; i++) {
				maxScores[i] = dataFile.readFloat();
				tt.increment();
			}
			tt.finished();
			dataFile.close();
		} else {
			logger.info("Caching max score values for maxscore structure into memory");
			msCache = CacheBuilder.newBuilder()
					              .maximumSize(CACHE_SIZE)
					              .<Integer, Float>build( new CacheLoader<Integer, Float>() {
					            	  	  	  @Override
					            	  	  	  public Float load(Integer key) throws IOException {
					            	  	  		  dataFile.seek(key * (Float.SIZE >> 3));
					            	  	  		  return dataFile.readFloat();
					            	  	  	  }
					              		  }
					              );
		}
	}
	
	public float getMaxScore(int termid)
	{
		if (maxScores != null)
			return maxScores[termid];
		else
			try {
				return msCache.get(termid);
			} catch (ExecutionException e) {
				System.err.println("Exception while accessing max score for termid " + termid + " while using cache. Returning Float.MAX_VALUE...");
				e.printStackTrace();
				return Float.MAX_VALUE;
			}
	}

	/*
	public static void WriteProperties(final IndexOnDisk index, String weightingModelClassName)
	{
		index.setIndexProperty("index.maxscore.class","it.cnr.isti.hpclab.maxscore.structures.MaxScoreIndex");
		index.setIndexProperty("index.maxscore.parameter_types","org.terrier.structures.Index");
		index.setIndexProperty("index.maxscore.parameter_values","index");
		
		if (weightingModelClassName.indexOf('.') == -1)
			weightingModelClassName = "it.cnr.isti.hpclab.matching.structures." + weightingModelClassName;
		index.setIndexProperty("index.maxscore.weighting_model", weightingModelClassName);
	}
	
	public static void WriteProperties(String indexPath, String indexPrefix, String weightingModelClassName) throws IOException
	{
		IndexOnDisk index = Index.createIndex(indexPath, indexPrefix);
		index.setIndexProperty("index.maxscore.class","it.cnr.isti.hpclab.maxscore.structures.MaxScoreIndex");
		index.setIndexProperty("index.maxscore.parameter_types","org.terrier.structures.Index");
		index.setIndexProperty("index.maxscore.parameter_values","index");
		
		if (weightingModelClassName.indexOf('.') == -1)
			weightingModelClassName = "it.cnr.isti.hpclab.matching.structures." + weightingModelClassName;
		index.setIndexProperty("index.maxscore.weighting_model", weightingModelClassName);

		index.close();
	}
	*/
	
	@Override
	public void close() throws IOException 
	{
		// TODO: find the correct way to close the resource when the actual index is closed
		// dataFile.close();
	}
}
