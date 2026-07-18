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

import java.io.IOException;

/**
 * Created by bryan on 2/4/16.
 */
public class IOExceptionUtil {
  public static IOException wrapIfNecessary( Throwable throwable ) {
    if ( throwable instanceof IOException ) {
      return (IOException) throwable;
    }
    return new IOException( throwable );
  }
}
