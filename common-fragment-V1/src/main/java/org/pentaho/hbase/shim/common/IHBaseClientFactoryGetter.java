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
package org.pentaho.hbase.shim.common;

import org.apache.hadoop.conf.Configuration;
import org.pentaho.hbase.factory.HBaseClientFactory;

public interface IHBaseClientFactoryGetter {
  HBaseClientFactory getHBaseClientFactory( Configuration configuration );
}
