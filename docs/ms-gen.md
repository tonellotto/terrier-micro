# Max score generator tool

Dynamic pruning strategies such as `MaxScore`, `Wand` and `BlockMaxWand` require a _max score_ value (a.k.a. term upper bound) associated to each term in the lexicon. The `ms-generate` tool appends a new data structure to an existing index, called **max score index**. The new data structure is file-stored array of floats, indexed by term id (extension: `.ms`). The array maps the lexicon termid to the corresponding _max score_. The tool modifies the index properties file to store information about the newly created _max score_ index.

To generate a _max score_ index, computing the _max score_ using the [BM25](../src/main/java/it/cnr/isti/hpclab/matching/structures/BM25.java) weighting model:

```bash
./target/bin/ms-generate -index /path/to/old/index/cw09b.properties -wm BM25
```

The index properties file now includes the weighting model used to generate the _max score_.

The `ms-generate` tool accepts the following options.

```
-index [String] (required)
```

Fully qualified filename of one of the files of a existing Terrier index. The parameter will be split automatically into a Terrier path and prefix.

```
-wm [String] (required)
```

Class implementing the *[weighting model](../src/main/java/it/cnr/isti/hpclab/matching/structures/WeightingModel.java)* to use when computing the _max score_.
If no *full* class name is provided, the `it.cnr.isti.hpclab.matching.structures.` string will be prepended to the argument given.
The currently implemented weighting models are [BM25](../src/main/java/it/cnr/isti/hpclab/matching/structures/BM25.java), [LM](../src/main/java/it/cnr/isti/hpclab/matching/structures/LM.java) and [DLH13](../src/main/java/it/cnr/isti/hpclab/matching/structures/DLH13.java).

```
-p [Number] (optional)
```

Number of threads to use. Anyway the maximum value will be the number of available cores. Default: 1.

**Multi-threaded compressions is experimental -- caution advised due to threads competing for available memory!**

## MaxScoreIndex usage

Once generated, the new **max score index** can be accessed and used as any other Terrier 5 index data structure:

```java
import it.cnr.isti.hpclab.maxscore.structures.MaxScoreIndex;
...
MaxScoreIndex msi = (MaxScoreIndex) index.getIndexStructure("maxscore");
...
```

When a index is opened for query processing, the _max score_ array is fully loaded in main memory; in order to save memory, it is possible to access it form file, leveraging a small cache memory with 10Ki entries. This behavior is controlled by the `preload.maxscore.index` system property (default: `true`).

## Notes

-   The weighting models used for _max score_ generation and batch query processing **must** be the same. An new index property is used to ensure this.
-   For memory problems, check the `appassembler` Maven plugin parameter in the [POM](../pom.xml) file

## Credits

Developed by Nicola Tonellotto, ISTI-CNR.
