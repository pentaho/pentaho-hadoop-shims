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

import java.io.IOException;

/**
 * Created by bryan on 1/20/16.
 */
public interface HBasePut {
  void setWriteToWAL( boolean writeToWAL );

  void addColumn( String columnFamily, String columnName, boolean colNameIsBinary, byte[] colValue ) throws
    IOException;

  String createColumnName( String... parts );

  void execute() throws IOException;
}
