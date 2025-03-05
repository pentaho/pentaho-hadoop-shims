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


package org.pentaho.hadoop.shim.api.hbase;

import java.util.NavigableMap;

/**
 * Created by bryan on 1/19/16.
 */
public interface Result {
  byte[] getRow();

  NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> getMap();

  NavigableMap<byte[], byte[]> getFamilyMap( String familyName );

  byte[] getValue( String colFamilyName, String colName, boolean colNameIsBinary );

  boolean isEmpty();
}
