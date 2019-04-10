package it.cnr.isti.hpclab.matching.structures.query;

import it.cnr.isti.hpclab.MatchingConfiguration;
import it.cnr.isti.hpclab.MatchingConfiguration.Property;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.stream.Collectors;

/** 
 * <p><b>Properties:</b>
 * <ul>
 * 		<li><tt>micro.queries.threshold</tt> a file containing the thresholds for priming. If queries have id, thresholds must have ids too</li>
 * </ul>
 */

public class ThresholdQuerySource extends TextQuerySource
{
	protected float[]  mQueryThresholds; /** The query threshold. */
		
	public ThresholdQuerySource() 
	{
		this(MatchingConfiguration.get(Property.QUERY_FILE));		
	}
	
	public ThresholdQuerySource(final File queryfile)
	{
		this(queryfile.getName());
	}
	
	public ThresholdQuerySource(final String queryfilename)
	{
		super(queryfilename);
		loadThresholds();
	}

	private void loadThresholds() 
	{
		mQueryThresholds = new float[mQueries.length];
		String line = null;
		int queryCount = 0, qidPos;

		if (MatchingConfiguration.get(Property.THRESHOLD_FILE).length() == 0)
			throw new IllegalStateException("Threshold file not specified");
		
		try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(MatchingConfiguration.get(Property.THRESHOLD_FILE))))) {
			while ((line = br.readLine()) != null) {
				line = line.trim();
				if (line.startsWith("#"))
					continue;
				String[] tokens = line.split("\\s+");
				if (!hasIds()) {
					if (line.contains(" "))
						throw new RuntimeException("Whitespaces in thresholds are not allowed");
					mQueryThresholds[queryCount++] = Float.parseFloat(line);
				} else {
					if (tokens.length != 2)
						throw new RuntimeException("Something wrong in thresholds... maybe missing qids or too many fields");
					qidPos = Arrays.stream(mQueryIds).boxed().collect(Collectors.toList()).indexOf(Integer.parseInt(tokens[0]));

					if (qidPos < 0)
						LOGGER.warn("No query corresponding to qid " + tokens[0] + " with threshold " + tokens[1]);
					else
						mQueryThresholds[qidPos] = Float.parseFloat(tokens[1]);
				}
			}
		} catch (IOException ioe) {
			LOGGER.error("IOException while extracting query thresholds: ",ioe);	
		}
	}

	public float getQueryThreshold()
	{
		if (mQueryIndex == 0)
			throw new UnsupportedOperationException();
		
		return mQueryThresholds[mQueryIndex - 1];
	}
}