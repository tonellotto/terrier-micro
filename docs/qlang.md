# Simple Query Language

By default, queries processed by Terrier Micro are simple terms, processed if necessary by the Terrier terms pipeline (stopword removal and/or Porter stemming).

The same term can appear multiple time in a query, and its weigthing model's score will be counted multiple times.

If a term is preceded by the character `+`, it **must** appear in the retrieved documents, so all documents without such term will be removed in the final top k list of results.

If a term has the form `<term>^1.33` (note the `^` character), its score contributions will be **multiplied** by the specificed weight, 1.33 in this case.
