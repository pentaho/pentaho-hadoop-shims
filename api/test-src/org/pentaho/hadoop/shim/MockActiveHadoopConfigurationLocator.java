/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2013 by Pentaho : http://www.pentaho.com
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

package org.pentaho.hadoop.shim;

import org.pentaho.hadoop.shim.api.ActiveHadoopConfigurationLocator;

public class MockActiveHadoopConfigurationLocator implements ActiveHadoopConfigurationLocator {

  private String activeId;

  public MockActiveHadoopConfigurationLocator() {
    this( null );
  }

  public MockActiveHadoopConfigurationLocator( String activeId ) {
    this.activeId = activeId;
  }

  @Override
  public String getActiveConfigurationId() throws ConfigurationException {
    return activeId;
  }

}
