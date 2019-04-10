package it.cnr.isti.hpclab.matching.structures.output;

import java.io.IOException;

import org.terrier.structures.Index;
import org.terrier.structures.MetaIndex;

import it.cnr.isti.hpclab.matching.structures.ResultSet;
import it.cnr.isti.hpclab.matching.structures.SearchRequest;
import it.cnr.isti.hpclab.util.Format;

public class DocnoResultOutput extends ScoredResultOutput
{
	protected MetaIndex mMetaIndex = null;
	
 	public DocnoResultOutput() 
 	{	
 		super();
 		this.mMetaIndex = Index.createIndex().getMetaIndex();
 		/*
 		try {
			// this.mMetaIndex = new CompressingMetaIndex(Index.createIndex(), "meta");
 	 		this.mMetaIndex = new ClueWeb09DocnoMetaIndex();
 			
		} catch (IOException e) {
			LOGGER.error("Exception while opening the meta index", e);
		}
		*/
	}
 	
	@Override
	public void print(final SearchRequest rq) throws IOException 
	{
		mBuilder.setLength(0); // reset the StringBuilder

		ResultSet resultSet = rq.getResultSet();
		if (resultSet.size() > 0) {
			// TODO : check that results are always ordered by score
			
			for (int i = 0; i < resultSet.size(); ++i)
				mBuilder.append(rq.getQueryId())
					 	.append('\t')
				        .append(mMetaIndex.getItem("docno", resultSet.docids()[i]))
				        .append('\t')
				        .append(Format.toString(resultSet.scores()[i], 5))
						.append('\n');
				        
			mWriter.print(mBuilder.toString());
		}
	}
}
