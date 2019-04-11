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
