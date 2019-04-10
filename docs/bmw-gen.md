# Block max score generator tool

Dynamic pruning strategies such as `BlockMaxWand` require a block max score data structure associated to each term in the lexicon. The `bmw-generate` tool appends a new data structure to an existing index with an existing *max score index*, called **block max score index**. The new data structure is file-stored array of blocks, indexed by term id and stored in three new files (extensions: `.bmw.docids`, `.bmw.scores`, `.bmw.offsets`). The array maps the lexicon termid to the corresponding maxscore [blocks](../src/main/java/it/cnr/isti/hpclab/maxscore/structures/Block.java). The tool modifies the index properties file to store information about the newly created block max score index.

To generate a block max score index, computing the block max scores *using the weighting model used to generate the max score index*, and with a block size of 128 postings:

```bash
	./target/bin/bmw-generate -index /path/to/old/index/cw09b.properties -b 128
```

The `bmw-generate` tools accepts the following options.

```
-index [String] (required)
```

Fully qualified filename of one of the files of a existing Terrier index. The parameter will be split automatically into a Terrier path and prefix.

```
-b [Number] (required)
```

The number of postings in a single block (typically, 64 or 128 posting, see the [Faster BlockMax WAND with Variable-sized Blocks](https://dl.acm.org/citation.cfm?id=3080780) paper for more details).
The index properties file now includes the blocxk size used to generate the _block max score_ index.

```
-p [Number] (optional)
```

Number of threads to use. Anyway the maximum value will be the number of available cores. Default: 1.

**Multi-threaded compressions is experimental -- caution advised due to threads competing for available memory!**

## BlockMaxScoreIndex usage

Once generated, the new **max score index** can be accessed and used as any other Terrier 5 index data structure:

```java
import it.cnr.isti.hpclab.maxscore.structures.BlockMaxScoreIndex;
...
BlockMaxScoreIndex bmsi = (BlockMaxScoreIndex) index.getIndexStructure("blockmaxscore");
...
```

When a index is opened for query processing, the block max score array is fully loaded in main memory.

## Notes

- for memory problems, check the `appassembler` Maven plugin parameter in the [POM](../pom.xml) file

## Credits

Developed by Nicola Tonellotto, ISTI-CNR.
