package org.pentaho.hadoop.shim.api.format;

import java.io.Closeable;

import org.pentaho.di.core.RowMetaAndData;

public interface RecordReader extends Iterable<RowMetaAndData>, Closeable {

}
