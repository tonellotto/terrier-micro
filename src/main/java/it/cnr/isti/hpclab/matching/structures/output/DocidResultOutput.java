package it.cnr.isti.hpclab.matching.structures.output;

import it.cnr.isti.hpclab.MatchingConfiguration.Property;
import it.cnr.isti.hpclab.matching.structures.ResultSet;
import it.cnr.isti.hpclab.matching.structures.SearchRequest;
import it.cnr.isti.hpclab.MatchingConfiguration;

import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.zip.GZIPOutputStream;

public class DocidResultOutput extends ResultOutput
{
	private PrintWriter mWriter;
 	private StringBuilder mBuilder;

 	public DocidResultOutput() 
 	{	
 		mBuilder = new StringBuilder();
		String resultsFilename = MatchingConfiguration.get(Property.RESULTS_FILENAME);

		try {
			BufferedOutputStream bos;
			bos = new BufferedOutputStream(new GZIPOutputStream(new FileOutputStream(resultsFilename)));
			mWriter = new PrintWriter(bos);
		} catch (FileNotFoundException e) {
			LOGGER.error("FileNotFoundException while opening "	+ resultsFilename, e);
		} catch (IOException e) {
			LOGGER.error("I/O issue while building PrintWriter", e);
		}
	}
 	
	@Override
	public void print(final SearchRequest rq) throws IOException 
	{
		mBuilder.setLength(0); // reset the StringBuilder

		ResultSet resultSet = rq.getResultSet();
		if (resultSet.size() > 0) {
			// TODO : check that results are always ordered by score
			for (int id: resultSet.docids())
				mBuilder.append(rq.getQueryId())
						.append('\t')
				        .append(id)
				        .append('\n');
				        
			mWriter.print(mBuilder.toString());
		}
	}

	@Override
	public void close() throws IOException 
	{
		mWriter.close();
	}
}
