/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/
package org.pentaho.di.job.entries.oozie;

/**
 * User: Dzmitry Stsiapanau Date: 02/18/2016 Time: 17:44
 */


import org.pentaho.hadoop.shim.ShimVersion;
import org.pentaho.hadoop.shim.spi.PentahoHadoopShim;

/**
 * This is an empty, fake class necessary for all shims because original this class comes at runtime from pentaho-big-data-plugin. So we add a fake one here so the
 * common tests will compile and run successfully.
 */
public class OozieClientFactoryImpl implements PentahoHadoopShim {
  /**
   * @return the version of this shim
   */
  @Override public ShimVersion getVersion() {
    return null;
  }
}

