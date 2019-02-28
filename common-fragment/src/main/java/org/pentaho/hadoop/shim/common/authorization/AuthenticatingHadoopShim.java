/*******************************************************************************
 * Pentaho Big Data
 * <p>
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
 * <p>
 * ******************************************************************************
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 ******************************************************************************/

package org.pentaho.hadoop.shim.common.authorization;

import com.google.common.annotations.VisibleForTesting;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.hadoop.shim.common.delegating.DelegatingHadoopShim;

public class AuthenticatingHadoopShim extends DelegatingHadoopShim {

  public static final String MAPPING_IMPERSONATION_TYPE = "pentaho.authentication.default.mapping.impersonation.type";



  @VisibleForTesting
  void createActivatorInstance( String className ) {
    try {
      Class.forName( className ).newInstance();
    } catch ( Exception e ) {
      LogChannel.GENERAL.logError( e.getMessage(), e );
    }
  }
}
