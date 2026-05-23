error id: file://<WORKSPACE>/src/main/java/com/nano/tsdb/core/StorageEngine.java:java/io/IOException#
file://<WORKSPACE>/src/main/java/com/nano/tsdb/core/StorageEngine.java
empty definition using pc, found symbol in pc: java/io/IOException#
empty definition using semanticdb
empty definition using fallback
non-local guesses:

offset: 44
uri: file://<WORKSPACE>/src/main/java/com/nano/tsdb/core/StorageEngine.java
text:
```scala
package com.nano.tsdb.core;

import java.io.@@IOException;
import java.util.List;

/**
 * Core storage engine interface.
 */
public interface StorageEngine {
    void write(String seriesId, long timestamp, double value) throws IOException;
    Double read(String seriesId, long timestamp) throws IOException;
    List<DataPoint> scan(String seriesId, long from, long to) throws IOException;
    void flush() throws IOException;
    void shutdown() throws IOException;
}

```


#### Short summary: 

empty definition using pc, found symbol in pc: java/io/IOException#