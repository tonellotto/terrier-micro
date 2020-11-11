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

package it.cnr.isti.hpclab.matching.structures.query;

import it.cnr.isti.hpclab.MatchingConfiguration;
import it.cnr.isti.hpclab.MatchingConfiguration.Property;
import it.cnr.isti.hpclab.util.Util;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Paths;

import static com.google.common.base.Preconditions.checkArgument;

import org.terrier.indexing.tokenisation.IdentityTokeniser;
import org.terrier.indexing.tokenisation.Tokeniser;

/** 
 * This class reads queries, in TREC format, from the specified file.
 * 
 * By default, queries are tokenised by this class,
 * and are passed verbatim to the query parser. Tokenisation can be turned off by the property
 * <tt>micro.query.tokenise</tt>, with the tokensier specified by <tt>tokeniser</tt>.
 * 
 * <p><b>Properties:</b>
 * <ul>
 *         <li><tt>micro.queries</tt> a comma separated list of filenames, containing the queries</li>
 *         <li><tt>micro.queries.tokenise</tt> (defaults to false). By default, the query is not passed through a tokeniser. If set to true, then it will be passed through the tokeniser configured by the <tt>tokeniser</tt> property.</li>
 *         <li><tt>micro.queries.num</tt> how many queries to process</li>
 * </ul>
 */

public class TrecQuerySource implements QuerySource
{
    protected String[] mQueries;  /** The queries in the query file. */
    protected String[] mQueryIds; /** The query identifiers in the query file. */
    
    protected int mQueryIndex = 0; /** Pointer to current read position in query array. */
    
    // read until finding a line that starts with the specified prefix
    private String read(BufferedReader reader, String prefix) throws IOException 
    {
        String line;
        while ( null != (line = reader.readLine()))
            if (line.startsWith(prefix))
                return line;            
       
        return null;
    }
    
    public TrecQuerySource() 
    {
        this(MatchingConfiguration.get(Property.QUERY_FILE));
    }
    
    public TrecQuerySource(final File queryfile)
    {
        this(queryfile.getName());
    }
    
    public TrecQuerySource(final String queryfilename)
    {
        try {
            checkArgument(queryfilename != null && queryfilename.length() > 0);

            int num_queries = Integer.MAX_VALUE;
            if (!MatchingConfiguration.get(Property.NUM_QUERIES).equals("all"))
                num_queries = Integer.parseInt(MatchingConfiguration.get(Property.NUM_QUERIES));

            extractQueries(queryfilename, num_queries);
        } catch (Exception ioe) {
            LOGGER.error("Problem getting the " + queryfilename + " file:", ioe);
            return;
        }
    }

    private boolean extractQueries(final String queryFilename, final int num_queries)
    {
        LOGGER.info("Extracting queries from TREC file " + queryFilename);

        ObjectList<String> queries = new ObjectArrayList<String>();
        ObjectList<String> qids    = new ObjectArrayList<String>();

        if (!Files.exists(Paths.get(queryFilename))) {
            LOGGER.error("The topics file " + queryFilename + " does not exist, or it cannot be read.");
            return false;
        }

        boolean gotSome = false;
        Tokeniser tokeniser = MatchingConfiguration.getBoolean(Property.MUST_TOKENISE) ? Tokeniser.getTokeniser() : new IdentityTokeniser();

        String line = null;
        String qid;
        String title;

        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(queryFilename)))) {
            while (null != (line = read(br, "<top>"))) {

                
                // Read the topic id
                line = read(br, "<num>");
                int qidBegin = line.indexOf(":");
                if (qidBegin == -1)
                    continue;
                qid = line.substring(qidBegin + 1).trim();
                
                // Read the topic title
                line = read(br, "<title>");
                int titleBegin = line.indexOf(":");
                if (titleBegin == -1)
                    titleBegin = line.indexOf(">");
                title = line.substring(titleBegin + 1).trim();

                // malformed titles, read again
                if (title.isEmpty()) {
                    line = read(br, "");
                    titleBegin = line.indexOf(":");
                    if (titleBegin == -1)
                        titleBegin = line.indexOf(">");
                 
                    title = line.substring(titleBegin + 1).trim();
                }        
                
                title = Util.JoinStrings(tokeniser.getTokens(new StringReader(title)), " ");
                
                queries.add(title);
                qids.add(qid);
                
                gotSome = true;
                LOGGER.debug("Extracted queryID " + qid + "  "+ title);
                
                if (queries.size() == num_queries)
                    break;
            }
        } catch (IOException ioe) {
            LOGGER.error("IOException while extracting queries: ",ioe);    
            return gotSome;
        }

        LOGGER.info("Extracted "+ queries.size() + " queries");

        this.mQueries = queries.toArray(new String[0]);
        this.mQueryIds = qids.toArray(new String[0]);

        return gotSome;
    }

    @Override
    public boolean hasNext() 
    {
        return mQueryIndex != mQueries.length;
    }

    @Override
    public String next() 
    {
        if (mQueryIndex == mQueries.length)
            return null;
        return mQueries[mQueryIndex++];
    }

    public String getQueryId()
    {
        if (mQueryIndex == 0)
            throw new UnsupportedOperationException();
        
        return mQueryIds[mQueryIndex - 1];
    }

    public void reset()
    {
        mQueryIndex = 0;
    }        
}