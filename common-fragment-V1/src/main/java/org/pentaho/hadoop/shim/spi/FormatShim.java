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

package org.pentaho.hadoop.shim.spi;


import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.format.IPentahoInputFormat;
import org.pentaho.hadoop.shim.api.format.IPentahoOutputFormat;

public interface FormatShim extends PentahoHadoopShim {
  <T extends IPentahoInputFormat> T createInputFormat( Class<T> type, NamedCluster namedCluster );

  <T extends IPentahoOutputFormat> T createOutputFormat( Class<T> type, NamedCluster namedCluster );
}
