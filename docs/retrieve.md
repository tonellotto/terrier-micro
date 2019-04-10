# Batch query processing

The `retrieve` tool is used to perform batch query processing experiments. To use it:

```shell
./target/bin/retrieve
```

## Configuration

The configuration of this script is completely managed by Java properties, as reported in the [MatchingConfiguration](../src/main/java/it/cnr/isti/hpclab/MatchingConfiguration) class. 

| Property name | Description |Default|
|---------------|-------------|-------|
| `micro.namespace`|default namespace to append to class names not including any namespace|`it.cnr.isti.hpclab.matching.`|
| `micro.index.path` | the name of the directory in which the index data structures to process are stored|`.`|
| `micro.index.prefix`| filename prefix for the index data structures to process|`data`|
| `micro.manager` |class name of the manager to use at runtime, created by the `Querying` class|`micro.namespace` + `Manager`|
| `micro.model`|class name of the weighting model to use at runtime, create by the `Manager` class |`micro.namespace` + `structures.BM25`|
| `micro.matching`| class name of the matching algorithm to use at runtime, create by the `Manager` class |`micro.namespace `+ `RankedOr`|
| `micro.queries`| comma-separated list of text file where queries are read from class `QuerySource`|` `|
| `micro.queries.id`|boolean value specifying if query lines contain an initial query id|`false`|
| `micro.queries.tokenise`|boolean value specifying if queries must be tokenised or treated as single terms|`true`|
| `micro.queries.lowercase`|boolean value specifying if queries must be lowercased|`true`|
| `micro.queries.num`|number of queries to process|`all`|
| `micro.queries.threshold`|file containing the thresholds for priming. If queries have id, thresholds must have ids too|`""`|
| `micro.termpipelines`|term processors to be applied in order to single terms |`Stopwords,PorterStemmer`|
| `micro.ignore.low.idf`|boolean value specifying if terms with low IDF must be ignored|`true`|
| `micro.topk`| the number of top documents to return, if necessary|`1000`|

The values can be changed by using Java command-line system property values:

```shell
    java -Dmicro.queries=/tmp/somefile.txt <...>
```
or by including a `micro.properties` file in the classpath.

## Query Format

Queries are read by the `QuerySource` class, one per line, **verbatim**, from the file(s) specified by the `micro.queries` property. Empty lines and lines starting with `#` are ignored. 
By default, queries are tokenised by this class, and are passed verbatim to the query parser. Tokenisation can be turned off by the property `micro.queries.tokenise`.
Moreover, the first token on each line can be the query id. This can be controlled by the property `micro.queries.id` (default: `false`).

## Matching Algorithms

A matching algorithm must implement the `MatchingAlgorithm` interface. The matching algorithms currently implemented are the following.

|Class|Description|
|-----|-----------|
|**`And`**      	|Boolean AND, scores are not computed, no results are actually returned.|
|**`Or`**       	|Boolean OR, scores are not computed, no results are actually returned.|
|**`RankedAnd`**	|Ranked AND processing, `micro.topk` documents returned with their scores.|
|**`RankedOr`** 	|Ranked OR processing, `micro.topk` documents returned with their scores, implemented as DAAT.|
|**`MaxScore`** 	|Ranked OR processing, `micro.topk` documents returned with their scores, implemented as Turtle & Flood's MaxScore.|
|**`Wand`**     	|Ranked OR processing, `micro.topk` documents returned with their scores, implemented as Carmel & Broder's WAND.|
|**`BlockMaxWand`**	|Ranked OR processing, `micro.topk` documents returned with their scores, implemented as Suel's BlockMaxWand (postings aligned).|

## Alternative usage

Use the provided Java program `Retrieve` as follows.
```shell
java it.cnr.isti.hpclab.Retrieve [y]
```
The program uses the Java properties to configure its runtime. The only parameter flag (`y`), if present, will print to stderr all configured properties with their values, waiting for confirmation to proceed. Otherwise, the query processing will begin, outputting results to stdout in a simple JSON format.
For example:
```shell
java -Xmx32G -server \
     -cp terrier-micro-1.4.0.jar \
     -Dmicro.index.path=/data1/khast/index-java \
     -Dmicro.index.prefix=cw09b.sux \
     -Dmicro.queries=./query_log/msn.1k.txt \
     -Dmicro.topk=30 \
     -Dmicro.matching=it.cnr.isti.hpclab.matching.RankedOr \
     -Dstopwords.filename=/home/khast/stuff/stopword-list.txt \
     it.cnr.isti.hpclab.Retrieve
```

## Output

During query processing, a JSON file is produced, one line per query. Every line contains information about the terms and the processing time of the query. It can be easily parsed with the [jq](https://stedolan.github.io/jq/) tool.

An additional output can be generate, if the actual docids returned for every query are necessary (typically, for debugging purposes or for effectiveness measures). This output is controlled by the following properties.

| Property name | Description |Default|
|---------------|-------------|-------|
|`micro.results.output.type`		|the type of output for results generation. Possible values are `null`, `docid`, `score`, `docno` and `trec` |`null`|
|`micro.results.filename`	 	|the absolute filename of the gzipped file where the output results will be stored						   |`results.gz`|

Depending on the output type, the content of the output type will be different, according to the following descriptions.

* `null`: no output is actually generated. This is the default behavior, since writing the results on file might negatively impact the efficiency of query processing.
* `docid`: each line of the output file has the format `<qid>TAB<docid>`.
* `score`: each line of the output file has the format `<qid>TAB<docid>TAB<score>`. The `score` value is ceiled to 5 decimal digits.
* `docno`: each line of the output file has the format `<qid>TAB<docno>TAB<score>`. The `score` value is ceiled to 5 decimal digits. The `docno` is retrieved from the metaindex.
* `trec`: each line of the output file has the format `<qid> Q0 <docno> <pos> <score> ISTI` (note the spaces instead of tabs and the 'immutable' strings `Q0` and `ISTI`). The `score` value is ceiled to 5 decimal digits. The `docno` is retrieved from the metaindex. The `pos` value is the position in the returned results array, starting from 0 (i.e., the highest scoring document is in position 0, etc.).
