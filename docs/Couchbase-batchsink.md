# Couchbase Batch Sink

Description
-----------
This sink writes to a Couchbase bucket.

Configuration
-------------

**Reference Name:** Name used to uniquely identify this source for lineage, annotating metadata, etc.

**Nodes:** List of nodes to use when connecting to the Couchbase cluster.

**Bucket:** Couchbase bucket name.

**Key Field:** Which input field should be used as the document identifier. Identifier is expected to be a string.

**Operation**: Type of write operation to perform. This can be set to Insert, Replace or Upsert. For more information,
see [Primitive Key-Value Operations].

[Primitive Key-Value Operations]:
https://docs.couchbase.com/java-sdk/2.7/core-operations.html#crud-overview

**Username:** User identity for connecting to the Couchbase.

**Password:** Password to use to connect to the Couchbase.

**Batch Size**: Size (in number of records) of the batched writes to the Couchbase bucket.
Each write to Couchbase contains some overhead. To maximize bulk write throughput,
maximize the amount of data stored per write. Commits of 1 MiB usually provide the best performance. Default value 
is 100 records. For more information, see [Batching Operations].

[Batching Operations]:
https://docs.couchbase.com/java-sdk/2.7/batching-operations.html

Data Types Mapping
----------

    | CDAP Schema Data Type | Couchbase Data Type   | Comment                                            |
    | --------------------- | --------------------- | -------------------------------------------------- |
    | boolean               | Boolean               |                                                    |
    | bytes                 | String                | base64 encoded String                              |
    | string                | String                |                                                    |
    | date                  | String                | Date string in the following format: yyyy-MM-dd    |
    | double                | Number                |                                                    |
    | decimal               | Number                |                                                    |
    | float                 | Number                |                                                    |
    | int                   | Number                |                                                    |
    | long                  | Number                |                                                    |
    | time                  | String                | Time string in the following format: HH:mm:ss.SSS  |
    | timestamp             | String                | Timestamp string in the following format:          |
    |                       |                       | yyyy-MM-ddTHH:mm:ss.SSSZ[UTC]                      |
    | array                 | Array                 |                                                    |
    | record                | Object                |                                                    |
    | enum                  | String                |                                                    |
    | map                   | Object                |                                                    |
    | union                 |                       | Depends on the actual value. For example, if it's  |
    |                       |                       | a union ["string","int","long"] and the value is   |
    |                       |                       | actually a long, the Couchbase document will have  |
    |                       |                       | the field as a Number. If a different record comes |
    |                       |                       | in with the value as a string, the Couchbase       |
    |                       |                       | document will end up with a String for that field. |
    
