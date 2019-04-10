package it.cnr.isti.hpclab;

import it.cnr.isti.hpclab.MatchingConfiguration.Property;
import it.cnr.isti.hpclab.matching.MatchingAlgorithm;
import it.cnr.isti.hpclab.matching.structures.Query.RuntimeProperty;
import it.cnr.isti.hpclab.matching.structures.output.ResultOutput;
import it.cnr.isti.hpclab.matching.structures.query.QuerySource;
import it.cnr.isti.hpclab.matching.structures.query.ThresholdQuerySource;
import it.cnr.isti.hpclab.matching.structures.ResultSet;
import it.cnr.isti.hpclab.matching.structures.SearchRequest;
import it.cnr.isti.hpclab.util.StatsLine;

import java.io.Closeable;
import java.io.IOException;

import org.apache.log4j.Logger;

import it.cnr.isti.hpclab.annotations.Managed;
import it.cnr.isti.hpclab.manager.Manager;

import static com.google.common.base.Preconditions.checkNotNull;
/**
 * FROM TERRIER
 * 
 * This class performs a batch mode retrieval from a set of TREC queries. 
 * <h2>Configuring</h2> 
 * <p>In the following, we list the main ways for configuring TRECQuerying,
 * before exhaustively listing the properties that can affect TRECQuerying.
 * 
 * <h3>Topics</h3> 
 * Files containing topics (queries to be evaluated) should be set using the <tt>trec.topics</tt> property.
 * Multiple topic files can be used together by separating their filenames using
 * commas. By default TRECQuerying assumes TREC tagged topic files, e.g.:
 * <pre>
 * &lt;top&gt;
 * &lt;num&gt; Number 1 &lt;/num&gt;
 * &lt;title&gt; Query terms &lt;/title&gt;
 * &lt;desc&gt; Description : A setence about the information need &lt;/desc&gt;
 * &lt;narr&gt; Narrative: More sentences about what is relevant or not&lt;/narr&gt;
 * &lt;/top&gt;
 * </pre>
 * If you have a topic files in a different format, you can used a differed
 * QuerySource by setting the property <tt>trec.topics.parser</tt>. For instance
 * <tt>trec.topics.parser=SingleLineTRECQuery</tt> should be used for topics
 * where one line is one query. See {@link org.terrier.applications.batchquerying.TRECQuery}
 * and {@link org.terrier.applications.batchquerying.SingleLineTRECQuery} for more information.
 * 
 * <h3>Models</h3> 
 * By default, Terrier uses the {@link InL2} retrieval model for all runs.
 * If the <tt>trec.model</tt> property is specified, then all runs will be made 
 * using that weighting model. You can change this by specifying another 
 * model using the property <tt>trec.model</tt>. E.g., to use 
 * {@link org.terrier.matching.models.PL2}, set <tt>trec.model=PL2</tt>. 
 * Similarly, when query expansion is enabled, the
 * default query expansion model is {@link Bo1}, controlled by the property
 * <tt>trec.qe.model</tt>.
 *
 * <h3>Result Files</h3> The results from the system are output in a trec_eval
 * compatable format. The filename of the results file is specified as the
 * WEIGHTINGMODELNAME_cCVALUE.RUNNO.res, in the var/results folder. RUNNO is
 * (usually) a constantly increasing number, as specified by a file in the
 * results folder. The location of the results folder can be altered by the
 * <tt>trec.results</tt> property. If the property <tt>trec.querycounter.type</tt>
 * is not set to sequential, the RUNNO will be a string including the time and a 
 * randomly generated number. This is best to use when many instances of Terrier 
 * are writing to the same results folder, as the incrementing RUNNO method is 
 * not mult-process safe (eg one Terrier could delete it while another is reading it). 
 * 
 * 
 * <h2>Properties</h2> 
 * <ul>
 * <li><tt>trec.topics.parser</tt> - the query parser that parses the topic file(s).
 * {@link TRECQuery} by default. Subclass the {@link TRECQuery} class and alter this property if
 * your topics come in a very different format to those of TREC. </li>
 * 
 * <li><tt>trec.topics</tt> - the name of the topic file. Multiple topics files can be used, if separated by comma. </li>
 * 
 * <li><tt>trec.model</tt> the name of the weighting model to be used during retrieval. Default InL2 </li>
 *<li><tt>trec.qe.model</tt> the name of the query expansino model to be used during query expansion. Default Bo1. </li>
 * 
 * <li><tt>c</tt> - the term frequency normalisation parameter value. A value specified at runtime as an
 * API parameter (e.g. TrecTerrier -c) overrides this property. 
 * 
 * <li><tt>trec.matching</tt> the name of the matching model that is used for
 * retrieval. Defaults to org.terrier.matching.taat.Full. </li>
 * 
 * <li><tt>trec.manager</tt> the name of the Manager that is used for retrieval. Defaults to Manager.</li> 
 * 
 * <li><tt>trec.results</tt> the location of the results folder for results.
 * Defaults to TERRIER_VAR/results/</li>
 * 
 * <li><tt>trec.results.file</tt> the exact result filename to be output. Defaults to an automatically generated filename - 
 * see <tt>trec.querycounter.type</tt>.</li>
 * 
  <li><tt>trec.querycounter.type</tt> - how the number (RUNNO) at the end of a run file should be generated. Defaults to sequential,
 * in which case RUNNO is a constantly increasing number. Otherwise it is a
 * string including the time and a randomly generated number.</li>  
 * 
 * <li><tt>trec.output.format.length</tt> - the very maximum number of results ever output per-query into the results file .
 * Default value 1000. 0 means no limit.</li> 
 * 
 * <li><tt>trec.iteration</tt> - the contents of the Iteration column in the
 * trec_eval compatible results. Defaults to 0. </li>
 * 
 * <li><tt>trec.querying.dump.settings</tt> - controls whether the settings used to
 * generate a results file should be dumped to a .settings file in conjunction
 * with the .res file. Defaults to true. 
 * 
 * <li><tt>trec.querying.outputformat</tt> - controls class to write the results file. Defaults to
 * {@link TRECDocnoOutputFormat}. Alternatives: {@link TRECDocnoOutputFormat}, {@link TRECDocidOutputFormat}, {@link NullOutputFormat}</li> 
 * 
 * <li><tt>trec.querying.outputformat.docno.meta.key</tt> - for {@link TRECDocnoOutputFormat}, defines the
 * MetaIndex key to use as the docno. Defaults to "docno".
 * 
 * <li><tt>trec.querying.resultscache</tt> - controls cache to use for query caching. 
 * Defaults to {@link NullQueryResultCache}</li> 
 * 
 * </ul>
 * 
 * @author Gianni Amati, Vassilis Plachouras, Ben He, Craig Macdonald, Nut Limsopatham
 */

/**
 * This class implements the "entry point" of a batch experiment for query processing.
 * It is responsible to parse the file containing the query stream, to process every query
 * in a Java object, to manage the results cache for already-processed queries, and to 
 * invoke the Manager component, responsible for driving the processing of every single query.
 * Eventually, it collects the results produced for every query and post-process them, for
 * correctness tests and/or for effectiveness evaluation.
 */
public class Querying implements Closeable
{	
	private static final Logger LOGGER = Logger.getLogger(Querying.class);	
	
	/** The number of matched queries. */
	protected int mMatchingQueryCount = 0;

	/** Data structures */
	protected QuerySource  mQuerySource;
	protected Manager      mManager;
	protected ResultOutput mResultOutput;
	
	public Querying() 
	{
		this.createQuerySource();
		this.createManager();
		this.createResultOutput();
	}
	
	protected void createManager() 
	{
		try {
			String matchingAlgorithmClassName =  MatchingConfiguration.get(Property.MATCHING_ALGORITHM_CLASSNAME);
			if (matchingAlgorithmClassName.indexOf('.') == -1)
				matchingAlgorithmClassName = MatchingConfiguration.get(Property.DEFAULT_NAMESPACE) + matchingAlgorithmClassName;
			String mManagerClassName = Class.forName(matchingAlgorithmClassName).asSubclass(MatchingAlgorithm.class).getAnnotation(Managed.class).by();
			mManager = (Manager) (Class.forName(mManagerClassName).asSubclass(Manager.class).getConstructor().newInstance());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	protected void createQuerySource() 
	{
		try {
			String querySourceClassName =  MatchingConfiguration.get(Property.QUERY_SOURCE_CLASSNAME);
			if (querySourceClassName.indexOf('.') == -1)
				querySourceClassName = MatchingConfiguration.get(Property.DEFAULT_NAMESPACE) + querySourceClassName;
			mQuerySource = (QuerySource) (Class.forName(querySourceClassName).asSubclass(QuerySource.class).getConstructor().newInstance());
				// this.mQuerySource = new QuerySource(queryFilenames);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
	
	protected void createResultOutput()
	{
		mResultOutput = ResultOutput.newInstance(MatchingConfiguration.get(Property.RESULTS_OUTPUT_TYPE));
	}

	@Override
	public void close() throws IOException 
	{
		mManager.close();
		mResultOutput.close();
	}

	public void processQueries() throws IOException 
	{
		mMatchingQueryCount = 0;
		mQuerySource.reset();

		final long startTime = System.currentTimeMillis();

		boolean doneSomeQueries = false;
		
		// iterating through the queries
		while (mQuerySource.hasNext()) {
			String query = mQuerySource.next();
			int qid   = mQuerySource.getQueryId();
			
			float  qth   = 0.0f;
			if (mQuerySource instanceof ThresholdQuerySource)
				qth = ((ThresholdQuerySource) mQuerySource).getQueryThreshold();
			
			// process the query
			long processingStartTime = System.currentTimeMillis();
			processQuery(qid, query, qth);
			long processingEndTime = System.currentTimeMillis();
			if (LOGGER.isInfoEnabled())
				LOGGER.info("Time to process query: " + ((processingEndTime - processingStartTime) / 1000.0D));
			doneSomeQueries = true;
		}
		if (doneSomeQueries)
			LOGGER.info("Finished topics, executed " + mMatchingQueryCount +
						" queries in " + ((System.currentTimeMillis() - startTime) / 1000.0d) +
						" seconds");
	}

	public void processQuery(final int queryId, final String query, final float threshold) throws IOException
	{
		checkNotNull(queryId);
		checkNotNull(query);
		
		if (LOGGER.isInfoEnabled())
			LOGGER.info(queryId + " : " + query);
		
		SearchRequest srq = new SearchRequest(queryId, query);
		srq.getQuery().addMetadata(RuntimeProperty.INITIAL_THRESHOLD, Float.toString(threshold));
		
		if (LOGGER.isInfoEnabled())
			LOGGER.info("Processing query: " + queryId + ": '" + query + "'");
		mMatchingQueryCount++;
		ResultSet rs = mManager.run(srq);
		srq.setResultSet(rs); //TODO: shouldn't this be inside runMatching?
		if (rs.size() != 0) {
			PrintStats(srq);
			mResultOutput.print(srq);
		}
	}

	private static void PrintStats(SearchRequest srq) 
	{
		StatsLine statsLine = new StatsLine();
		
		statsLine.add("type", "\"query\"");
		statsLine.add("qid", Integer.toString(srq.getQueryId()));
		
		for (RuntimeProperty prop: RuntimeProperty.values())
			if (srq.getQuery().getMetadata(prop) != null)
				statsLine.add(prop.toString(), srq.getQuery().getMetadata(prop));

		statsLine.print();
	}
}
