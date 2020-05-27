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

package it.cnr.isti.hpclab.maxscore;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.function.BinaryOperator;

import org.terrier.structures.Index;

import it.cnr.isti.hpclab.maxscore.structures.BlockMaxScoreIndex;
import it.cnr.isti.hpclab.ef.TermPartition;

public class BMWReducer implements BinaryOperator<TermPartition> 
{
    private final String dst_index_path;
    private final String dst_index_prefix;
    
    public BMWReducer(final String dst_index_path, final String dst_index_prefix)
    {
        this.dst_index_path = dst_index_path;
        this.dst_index_prefix = dst_index_prefix;
    }

    @Override
    public TermPartition apply(TermPartition t1, TermPartition t2) 
    {
        Index.setIndexLoadingProfileAsRetrieval(false);
        String out_prefix = this.dst_index_prefix + "_merge_" + t1.id();
        
        try {
            // Merge docids (low level)
            long docid_offset = merge(t1.prefix() + BlockMaxScoreIndex.STRUCTURE_NAME + BlockMaxScoreIndex.DOCID_EXT, 
                                      t2.prefix() + BlockMaxScoreIndex.STRUCTURE_NAME + BlockMaxScoreIndex.DOCID_EXT,
                                      out_prefix  + BlockMaxScoreIndex.STRUCTURE_NAME + BlockMaxScoreIndex.DOCID_EXT);
        
            // Merge scores (low level)
            long score_offset = merge(t1.prefix() + BlockMaxScoreIndex.STRUCTURE_NAME + BlockMaxScoreIndex.SCORE_EXT,
                                      t2.prefix() + BlockMaxScoreIndex.STRUCTURE_NAME + BlockMaxScoreIndex.SCORE_EXT, 
                                      out_prefix  + BlockMaxScoreIndex.STRUCTURE_NAME + BlockMaxScoreIndex.SCORE_EXT);

            if (docid_offset / Integer.BYTES != score_offset / Float.BYTES)
                throw new IllegalStateException();
            
            final long written_entries = docid_offset / Integer.BYTES;
            
            // Merge offsets (inplace t1 merge with t2 while recomputing offsets)
            DataOutputStream offsetsOutput = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(this.dst_index_path + File.separator + t1.prefix() + BlockMaxScoreIndex.STRUCTURE_NAME + BlockMaxScoreIndex.OFFSET_EXT, true)));
            DataInputStream offsetsInput   = new DataInputStream(new BufferedInputStream(new FileInputStream(this.dst_index_path + File.separator + t2.prefix() + BlockMaxScoreIndex.STRUCTURE_NAME + BlockMaxScoreIndex.OFFSET_EXT)));
            
            try {
                while (true)
                    offsetsOutput.writeLong(offsetsInput.readLong() + written_entries);
            } catch (EOFException e) {
                // e.printStackTrace();
            }
            
            offsetsInput.close();
            offsetsOutput.close();
                        
            Files.move(Paths.get(this.dst_index_path, t1.prefix() + BlockMaxScoreIndex.STRUCTURE_NAME + BlockMaxScoreIndex.OFFSET_EXT),
                       Paths.get(this.dst_index_path, out_prefix  + BlockMaxScoreIndex.STRUCTURE_NAME + BlockMaxScoreIndex.OFFSET_EXT));
            Files.delete(Paths.get(dst_index_path,    t2.prefix() + BlockMaxScoreIndex.STRUCTURE_NAME + BlockMaxScoreIndex.OFFSET_EXT));
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Set correct prefix for next merging and return it
        t1.prefix(out_prefix);
        t1.id(t2.id());
        return t1;
    }    
    
    private long merge(final String prefix_in1, final String prefix_in2, final String out_prefix) throws IOException 
    {
        Path in_file_1 = Paths.get(this.dst_index_path + File.separator + prefix_in1);
        Path in_file_2 = Paths.get(this.dst_index_path + File.separator + prefix_in2);
        Path out_file  = Paths.get(this.dst_index_path + File.separator + out_prefix);

        final long offset = Files.size(in_file_1);
        try (FileChannel out = FileChannel.open(in_file_1, StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
            try (FileChannel in = FileChannel.open(in_file_2, StandardOpenOption.READ)) {
                long l = in.size();
                for (long p = 0; p < l; )
                    p += in.transferTo(p, l - p, out);
            }
        }
        
        Files.move(in_file_1, out_file);
        Files.delete(in_file_2);
        return offset;
    }
}