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

import com.pentaho.di.automation.PluginTestDockerUtils;
import org.junit.jupiter.api.BeforeAll;

import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

abstract class PentahoKtrIT {

  private static final String TRANSFORMATION_PATH = "transformations/pdi-it/output.ktr";
  private static final String OUTPUT_PATH = "/tmp/pentaho-ktr-it/output.txt";

  protected static PluginTestDockerUtils dockerUtils;

  @BeforeAll
  static void setUpDockerRunner() {
    dockerUtils = new PluginTestDockerUtils();
  }

  protected final void assertTransformationOutput() throws Exception {
    String containerId = dockerUtils.getPdiContainerId();
    dockerUtils.runDockerExecCmd( containerId, true, null,
      "rm -f /tmp/pentaho-ktr-it/output /tmp/pentaho-ktr-it/output.txt" );

    dockerUtils.runTransformationAndExamineOutput( true, Collections.emptyList(), "Basic",
      TRANSFORMATION_PATH, Map.of() );

    String output = dockerUtils.runDockerExecCmd( containerId, true, null, "cat " + OUTPUT_PATH );
    assertEquals( "alpha;1\nbeta;2", output.replace( "\r\n", "\n" ).trim() );
  }
}