Inverted Index Builder
====================

A helper library could be used to build Inverted Index files under memory limitation.

Features
----
 - Building Inverted Index into multiple files.
 - Secondary Indexing for Inverted Index files.
 - Small* memory footprint

*Depends on how many articles you are building with.

Performance
----

 * Environment

Item|Value
-------- | ---
CPU|Core i7 3612QM
Operating System|Ubuntu 14.04.01 LTS 64Bit
JDK/JRE|OpenJDK Runtime Environment (IcedTea6 1.13.5)
JRE Arguments|-Xmx512m

 * Test Configuration

Item|Value
-------- | ---
Corpus|10,228 pages from Wikipedia
Distinct Token Count|1,600,000+
Index Algorithm|Occurrence (No Compression)

 * Result

Heap Space|Batch Size|Time|Index Size
-------- | ---|---|---
-Xmx512m|1,750|178.044s|353.8MB

Usage
----

Example can be found in "src/org/owwlo/InvertedIndexing/examples".

```java
// Specify directory for index files.
File dir = new File(System.getProperty("java.io.tmpdir"));

//Create a new instance of InvertedIndexBuilder.
InvertedIndexBuilder builder = InvertedIndexBuilder.getBuilder(dir);

// Create a new Inverted Index Map for one batch of text files.
IvtMapInteger ivtMapBatch = builder.createDistributedIvtiIntegerMap();

/** Build Index into a Map<String, Integer> here. Say "ivtMap" **/

// Put all index items into DistributedIvtiIntegerMap.
ivtMapBatch.putAll(ivtMap);

// Highly recommend. You don't want memory leaking.
ivtMapBatch.close();

// Close this instance of InvertedIndexBuilder.
// This will write everything into index directory.
builder.close();

```


License
----

The MIT License (MIT)

Copyright (c) 2014 owwlo

Licensed under the MIT License. See the top-level file LICENSE.


*双汇王中王鲜肉火腿肠！*
