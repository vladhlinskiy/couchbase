# Couchbase Batch Source

Description
-----------
Reads documents from a Couchbase bucket and converts each document into a StructuredRecord with the help
of a specified schema.

Configuration
-------------

**Reference Name:** Name used to uniquely identify this source for lineage, annotating metadata, etc.

**Nodes:** List of nodes to use when connecting to the Couchbase cluster.

**Bucket:** Couchbase bucket name.

**Input Query:** N1QL query to use to import data from the specified bucket. For more information, 
see [N1QL Language Reference].

[N1QL Language Reference]:
https://docs.couchbase.com/server/current/n1ql/n1ql-language-reference/index.html

**Username:** User identity for connecting to the Couchbase.

**Password:** Password to use to connect to the Couchbase.

**Output Schema:** Specifies the schema of the documents.


Data Types Mapping
----------

    | Couchbase Data Type             | CDAP Schema Data Type                             |
    | ------------------------------- | ------------------------------------------------- |
    | Boolean                         | boolean                                           |
    | Number                          | int, long, double, decimal                        |
    | String                          | string                                            |
    | Object                          | record, map                                       |
    | Array                           | array                                             |
    
