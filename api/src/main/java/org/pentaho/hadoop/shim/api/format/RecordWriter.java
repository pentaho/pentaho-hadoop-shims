package org.pentaho.hadoop.shim.api.format;

import java.io.Closeable;

import org.pentaho.di.core.RowMetaAndData;

public interface RecordWriter extends Closeable {

  void write( RowMetaAndData row );
}
