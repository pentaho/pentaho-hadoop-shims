/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/
package org.pentaho.hbase.factory;

public interface HBasePut {
  void setWriteToWAL( boolean writeToWAL ) throws Exception;

  void addColumn( byte[] colFamily, byte[] colName, byte[] colValue );

}
