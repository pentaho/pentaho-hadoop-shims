/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


package org.pentaho.hadoop.shim.api.hbase.table;

import org.pentaho.hadoop.shim.api.hbase.mapping.Mapping;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.variables.VariableSpace;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

/**
 * Created by bryan on 1/19/16.
 */
public interface HBaseTable extends Closeable {
  boolean exists() throws IOException;

  boolean disabled() throws IOException;

  boolean available() throws IOException;

  void disable() throws IOException;

  void enable() throws IOException;

  void delete() throws IOException;

  void create( List<String> colFamilyNames, Properties creationProps ) throws IOException;

  ResultScannerBuilder createScannerBuilder( byte[] keyLowerBound, byte[] keyUpperBound );

  ResultScannerBuilder createScannerBuilder( Mapping tableMapping, String dateOrNumberConversionMaskForKey,
                                             String keyStartS, String keyStopS,
                                             String scannerCacheSize, LogChannelInterface log, VariableSpace vars )
    throws KettleException;

  List<String> getColumnFamilies() throws IOException;

  boolean keyExists( byte[] key ) throws IOException;

  HBaseTableWriteOperationManager createWriteOperationManager( Long writeBufferSize ) throws IOException;
}
