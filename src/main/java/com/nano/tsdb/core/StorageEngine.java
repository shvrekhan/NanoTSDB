package com.nano.tsdb.core;

import java.io.IOException;
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
