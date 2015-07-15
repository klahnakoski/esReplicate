esReplicate
===========

Replicate an ES index to another cluster, and keep it up to date.


Assumptions
-----------

The index must have a `primary_property` such that each new document has a value strictly greater than all currently index documents.
Timestamps, to the millisecond are a good example.
