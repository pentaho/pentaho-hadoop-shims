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


package org.pentaho.hadoop.shim.api.format;

import org.pentaho.hadoop.shim.api.cluster.NamedCluster;

public interface FormatService {

  <T extends IPentahoInputFormat> T createInputFormat( Class<T> type, NamedCluster namedCluster );

  <T extends IPentahoOutputFormat> T createOutputFormat( Class<T> type, NamedCluster namedCluster );
}
