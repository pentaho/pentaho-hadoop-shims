/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2026 by Pentaho Canada Inc. : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.txt file.
 *
 * Change Date: 2030-06-15
 ******************************************************************************/
package org.pentaho.hadoop.shims.integration;

import org.junit.jupiter.api.Test;

class Pentaho102KtrIT extends PentahoKtrIT {

  @Test
  void executesKtrAndAssertsOutput() throws Exception {
    assertTransformationOutput();
  }
}