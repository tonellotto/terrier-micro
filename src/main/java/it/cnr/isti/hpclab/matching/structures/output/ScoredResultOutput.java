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

import it.cnr.isti.hpclab.MatchingConfiguration.Property;
import it.cnr.isti.hpclab.matching.structures.ResultSet;
import it.cnr.isti.hpclab.matching.structures.SearchRequest;
import it.cnr.isti.hpclab.MatchingConfiguration;
import it.cnr.isti.hpclab.util.Format;

import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.zip.GZIPOutputStream;

public class ScoredResultOutput extends ResultOutput
{
	protected PrintWriter mWriter;
 	protected StringBuilder mBuilder;

 	public ScoredResultOutput() 
 	{	
 		mBuilder = new StringBuilder();
		String resultsFilename = MatchingConfiguration.get(Property.RESULTS_FILENAME);

		try {
			BufferedOutputStream bos;
			if (resultsFilename.endsWith(".gz"))
				bos = new BufferedOutputStream(new GZIPOutputStream(new FileOutputStream(resultsFilename)));
			else
				bos = new BufferedOutputStream(new FileOutputStream(resultsFilename));
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
			for (int i = 0; i < resultSet.size(); ++i)
				mBuilder.append(rq.getQueryId())
					 	.append('\t')
				        .append(resultSet.docids()[i])
				        .append('\t')
				        .append(Format.toString(resultSet.scores()[i], 5))
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
