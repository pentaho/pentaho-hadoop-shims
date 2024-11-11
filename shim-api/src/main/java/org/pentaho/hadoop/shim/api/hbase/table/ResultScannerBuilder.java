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

import org.pentaho.hadoop.shim.api.hbase.mapping.ColumnFilter;
import org.pentaho.hadoop.shim.api.hbase.meta.HBaseValueMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;

import java.io.IOException;

/**
 * Created by bryan on 1/19/16.
 */
public interface ResultScannerBuilder {
  void addColumnToScan( String colFamilyName, String colName, boolean colNameIsBinary ) throws IOException;

  void addColumnFilterToScan( ColumnFilter cf, HBaseValueMetaInterface columnMeta, VariableSpace vars,
                              boolean matchAny )
    throws IOException;

  void setCaching( int cacheSize );

  ResultScanner build() throws IOException;
}
