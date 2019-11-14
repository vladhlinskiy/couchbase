# Couchbase Batch Source

Description
-----------
Reads documents from a Couchbase bucket. A filter can be specified to only output documents that meet a specific
criteria.

Configuration
-------------

**Reference Name:** Name used to uniquely identify this source for lineage, annotating metadata, etc.

**Nodes:** List of nodes to use when connecting to the Couchbase cluster.

**Bucket:** Couchbase bucket name.

**Select Fields:** Comma-separated list of fields to be read.

**Conditions:** Optional criteria (filters or predicates) that the result documents must satisfy. Corresponds to
the [WHERE clause] in [N1QL SELECT statement].

[WHERE clause]:
https://docs.couchbase.com/server/current/n1ql/n1ql-language-reference/where.html

[N1QL SELECT statement]:
https://docs.couchbase.com/server/current/n1ql/n1ql-language-reference/selectintro.html

**Number of Splits:** Desired number of splits to divide the query into when reading from Couchbase.
Fewer splits may be created if the query cannot be divided into the desired number of splits. If the specified value is
zero, the plugin will use the number of map tasks as the number of splits. Note, that there may be performance
implications in the case when the specified value is greater than 1 and the conditions do not use indexed fields.
For more information, see [Pagination Pushdown].

[Pagination Pushdown]:
https://docs.couchbase.com/server/6.0/learn/services-and-indexes/indexes/index_pushdowns.html#pagination-pushdown

**Username:** User identity for connecting to the Couchbase.

**Password:** Password to use to connect to the Couchbase.

**Output Schema:** Specifies the schema of the documents.

**Sample Size:** Specifies the number of documents to randomly sample in the bucket when inferring the schema. 
The default sample size is 1000 documents. If a bucket contains fewer documents than the specified number, then all 
the documents in the bucket will be used.

**Max Parallelism:** Maximum number of CPU cores to be used to process a query. If the specified value is less than
zero or greater than the total number of cores in a cluster, the system will use all available cores in the cluster.
For more information, see [Parallelism Parameter].

[Parallelism Parameter]:
https://docs.couchbase.com/server/6.0/analytics/appendix_2_parameters.html#Parallelism_parameter

**Scan Consistency:** Specifies the consistency guarantee or constraint for index scanning. For more information,
see [N1QL REST API].

[N1QL REST API]:
https://docs.couchbase.com/server/6.0/n1ql/n1ql-rest-api/index.html#table_xmr_grl_lt

**Query Timeout:** Number of seconds to wait before a timeout has occurred on a query. The pipeline will fail if
the timeout is exceeded.

Data Types Mapping
----------

    | Couchbase Data Type             | CDAP Schema Data Type              | Comment
    | ------------------------------- | ---------------------------------- | --------------------------- |
    | Boolean                         | boolean                            |                             |
    | Number                          | string, int, long, double, decimal | Mapped to string by default |
    | String                          | string                             |                             |
    | Object                          | record, map                        |                             |
    | Array                           | array                              |                             |
    
