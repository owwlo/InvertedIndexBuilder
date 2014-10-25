InvertedIndexBuilder
====================

A helper library could be used to build Inverted Index files under memory limitation.

Features
----
 - Building Inverted Index into multiple files.
 - Secondary Indexing for Inverted Index files.
 - Small* memory footprint

*Depends on how many passages you are building with.

Performance
----

lalala

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
