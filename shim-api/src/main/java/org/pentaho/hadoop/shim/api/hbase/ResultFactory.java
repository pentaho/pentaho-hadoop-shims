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

/**
 * Created by bryan on 1/29/16.
 */
public interface ResultFactory {
  boolean canHandle( Object object );

  Result create( Object object ) throws ResultFactoryException;
}
