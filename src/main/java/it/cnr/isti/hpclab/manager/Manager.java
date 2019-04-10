package it.cnr.isti.hpclab.manager;

import java.io.Closeable;
import java.io.IOException;

import org.apache.log4j.Logger;
import org.terrier.structures.Index;

import it.cnr.isti.hpclab.MatchingConfiguration;
import it.cnr.isti.hpclab.MatchingConfiguration.Property;
import it.cnr.isti.hpclab.matching.MatchingAlgorithm;
import it.cnr.isti.hpclab.matching.structures.ResultSet;
import it.cnr.isti.hpclab.matching.structures.SearchRequest;
import it.cnr.isti.hpclab.matching.structures.WeightingModel;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * This class implements the component actually driving the low-level query processing activities.
 * It basically manages the objects and configurations shared by all queries in a batch experiments,
 * and invokes the query processing algorithm.
 */
public abstract class Manager implements Closeable
{
	protected static final Logger LOGGER = Logger.getLogger(Manager.class);
	protected static boolean IGNORE_LOW_IDF_TERMS = MatchingConfiguration.getBoolean(Property.IGNORE_LOW_IDF_TERMS);
	
	protected Index mIndex = null;
	public WeightingModel mWeightingModel = null;
	protected MatchingAlgorithm mMatchingAlgorithm = null;

	public long processingTime;
	public long processedPostings;
	
	public long partiallyProcessedDocuments;
	public long numPivots;

	public Manager()
	{
		this(null);
	}

	public Manager(final Index index)
	{
		String weightingModelClassName = MatchingConfiguration.get(Property.WEIGHTING_MODEL_CLASSNAME);
		createWeigthingModel(weightingModelClassName);
		
		String matchingAlgorithmClassName =  MatchingConfiguration.get(Property.MATCHING_ALGORITHM_CLASSNAME); 
		createMatchingAlgorithm(matchingAlgorithmClassName);
		
		loadIndex(index);
	}

	public Manager(final Index index, final String matchingAlgorithmClassName)
	{
		String weightingModelClassName = MatchingConfiguration.get(Property.WEIGHTING_MODEL_CLASSNAME);
		createWeigthingModel(weightingModelClassName);
		 
		createMatchingAlgorithm(matchingAlgorithmClassName);
		
		loadIndex(index);
	}

	public Manager(final Index index, final String weightingModelClassName, final String matchingAlgorithmClassName)
	{
		createWeigthingModel(weightingModelClassName);	
		createMatchingAlgorithm(matchingAlgorithmClassName);
		
		loadIndex(index);
	}

	protected void loadIndex(final Index index) 
	{
		if (index == null)
			mIndex = Index.createIndex();
		else 
			mIndex = index;
		if (mIndex == null) {
			LOGGER.fatal("Failed to load index. " + Index.getLastIndexLoadError());
			throw new IllegalArgumentException("Failed to load index: " + Index.getLastIndexLoadError());
		}
		mWeightingModel.setup(mIndex);
		// mMatchingAlgorithm.setup(mIndex, mWeightingModel);
		mMatchingAlgorithm.setup(this);
	}

	protected void createWeigthingModel(String weightingModelClassName) 
	{
		checkNotNull(weightingModelClassName);
		try {
			if (weightingModelClassName.indexOf('.') == -1)
				weightingModelClassName = MatchingConfiguration.get(Property.DEFAULT_NAMESPACE) + "structures.model." + weightingModelClassName;
			mWeightingModel = (WeightingModel) (Class.forName(weightingModelClassName).asSubclass(WeightingModel.class).getConstructor().newInstance());
		} catch (Exception e) {
			LOGGER.error("Problem loading weighting model (" + weightingModelClassName + "): ", e);
		}
	}

	protected void createMatchingAlgorithm(String matchingAlgorithmClassName) 
	{
		checkNotNull(matchingAlgorithmClassName);
		try {
			if (matchingAlgorithmClassName.indexOf('.') == -1)
				matchingAlgorithmClassName = MatchingConfiguration.get(Property.DEFAULT_NAMESPACE) + matchingAlgorithmClassName;
			mMatchingAlgorithm = (MatchingAlgorithm) (Class.forName(matchingAlgorithmClassName).asSubclass(MatchingAlgorithm.class).getConstructor().newInstance());
		} catch (Exception e) {
			LOGGER.error("Problem loading matching algorithm (" + matchingAlgorithmClassName + "): ", e);
		}
	}

	@Override
	public void close() throws IOException 
	{
		mIndex.close();
	}

	abstract public ResultSet run(final SearchRequest srq) throws IOException;
	
	public final int TOP_K() 
	{ 
		return MatchingConfiguration.getInt(Property.TOP_K); 
	}	
	
	abstract public void reset_to(final int to) throws IOException;
}
