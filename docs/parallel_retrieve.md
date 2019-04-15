# Parallel Batch query processing

The `parallel-retrieve` tool is used to perform batch query processing experiments using all available threads. To use it:

```shell
./target/bin/parallel-retrieve
```

The configuration, query format, matching algorithms and output are identical to the [retrieve](./retrieve.md) tool.

This tool starts a query processing thread per available processor on the target machines, together with a thread to store the results and the main program thread, responsible for coordination and reading queries.
All query processing threads share the same configuration and query processing algorithm. Communication among threads is carried out through Java's `BlockingQueue`s.

A global shared queue manages all _search requests_, that are processed by _processing threads_ as soon as they are idle. When a search request is processed, it is managed by another shared queue, responsible for output generation.

Communications, as well as query processing, is completely asyncrhonous, so there is **no ordering guarantee** that queries will be processed, and their output generated, as they are read from the query source.
