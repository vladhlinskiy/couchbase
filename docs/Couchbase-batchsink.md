# Couchbase Batch Sink

Description
-----------
This sink writes to a Couchbase bucket.

Configuration
-------------

**Reference Name:** Name used to uniquely identify this source for lineage, annotating metadata, etc.

**Nodes:** List of nodes to use when connecting to the Couchbase cluster.

**Bucket:** Couchbase bucket name.

**ID Field:** Which input field should be used as the document identifier. Identifier is expected to be a string.

**Username:** User identity for connecting to the Couchbase.

**Password:** Password to use to connect to the Couchbase.

Data Types Mapping
----------

    | CDAP Schema Data Type | Couchbase Data Type   | Comment                                            |
    | --------------------- | --------------------- | -------------------------------------------------- |
    | boolean               | Boolean               |                                                    |
    | bytes                 | String                | base64 encoded String                              |
    | string                | String                |                                                    |
    | date                  | String                | TODO format                                        |
    | double                | Number                |                                                    |
    | decimal               | Number                |                                                    |
    | float                 | Number                |                                                    |
    | int                   | Number                |                                                    |
    | long                  | Number                |                                                    |
    | time                  | String                | Time string in the following format: HH:mm:ss.SSS  |
    | timestamp             | String                | TODO format                                        |
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
    
