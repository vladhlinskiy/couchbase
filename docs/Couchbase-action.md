# Couchbase Action

Description
-----------
Action that runs a N1QL query.

Use Case
--------
The action can be used whenever you want to run a N1QL query before or after a data pipeline.
For example, you may want to run a N1QL update query on a bucket before the pipeline source pulls data from tables.

Properties
----------

**Nodes:** List of nodes to use when connecting to the Couchbase cluster.

**Bucket:** Couchbase bucket name.

**Query:** N1QL query to run.

**Username:** User identity for connecting to the Couchbase.

**Password:** Password to use to connect to the Couchbase.

**Max Parallelism:** Maximum number of CPU cores to be used to process a query. If the specified value is less than
zero or greater than the total number of cores in a cluster, the system will use all available cores in the cluster.
For more information, see [Parallelism Parameter].

[Parallelism Parameter]:
https://docs.couchbase.com/server/6.0/analytics/appendix_2_parameters.html#Parallelism_parameter

**Scan Consistency:** Specifies the consistency guarantee or constraint for index scanning. For more information,
see [N1QL REST API].

[N1QL REST API]:
https://docs.couchbase.com/server/6.0/n1ql/n1ql-rest-api/index.html#table_xmr_grl_lt

**Query Timeout:** Number of seconds to wait before a timeout has occurred on a query.
