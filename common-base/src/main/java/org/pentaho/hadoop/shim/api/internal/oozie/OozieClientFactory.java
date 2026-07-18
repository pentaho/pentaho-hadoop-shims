/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2002 - 2026 by Pentaho Canada Inc. : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2030-06-15
 ******************************************************************************/

package org.pentaho.hadoop.shim.api.internal.oozie;

import org.pentaho.hadoop.shim.spi.PentahoHadoopShim;

public interface OozieClientFactory extends PentahoHadoopShim {
  public OozieClient create( String oozieUrl );
}