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

package org.pentaho.hadoop.shim.api.internal.process;

import org.pentaho.hadoop.shim.api.internal.Configuration;

/**
 * User: Dzmitry Stsiapanau Date: 01/29/2016 Time: 18:06
 */
public interface Processable {
  void process( Configuration configuration );
}