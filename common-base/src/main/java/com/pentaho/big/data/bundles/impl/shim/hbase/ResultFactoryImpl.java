/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 - 2026 by Pentaho Canada Inc. : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2030-06-15
 ******************************************************************************/



package com.pentaho.big.data.bundles.impl.shim.hbase;

import org.pentaho.hadoop.shim.api.hbase.ResultFactory;
import org.pentaho.hadoop.shim.api.hbase.ResultFactoryException;
import org.pentaho.hadoop.shim.api.internal.hbase.HBaseBytesUtilShim;

/**
 * Created by bryan on 1/29/16.
 */
public class ResultFactoryImpl implements ResultFactory {
  private final HBaseBytesUtilShim hBaseBytesUtilShim;

  public ResultFactoryImpl( HBaseBytesUtilShim hBaseBytesUtilShim ) {
    this.hBaseBytesUtilShim = hBaseBytesUtilShim;
  }

  @Override public boolean canHandle( Object object ) {
    return object == null || object instanceof org.apache.hadoop.hbase.client.Result;
  }

  @Override public ResultImpl create( Object object ) throws ResultFactoryException {
    if ( object == null ) {
      return null;
    }
    try {
      return new ResultImpl( (org.apache.hadoop.hbase.client.Result) object, hBaseBytesUtilShim );
    } catch ( ClassCastException e ) {
      throw new ResultFactoryException( e );
    }
  }
}
