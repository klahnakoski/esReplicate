# esReplicate

Replicate an ES index to another cluster, and keep it up to date.

## Status

This project is retired. Elasticsearch has a `_reindex` command, which subsumes much of the code here. 



## Assumptions

The index must have a `primary_property` such that each new document has a value strictly greater than all currently index documents.
Timestamps, to the millisecond are a good example.
