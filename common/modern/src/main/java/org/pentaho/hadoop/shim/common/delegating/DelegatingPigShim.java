/*******************************************************************************
*
* Pentaho Big Data
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

package org.pentaho.hadoop.shim.common.delegating;

import org.pentaho.hadoop.shim.ShimVersion;
import org.pentaho.hadoop.shim.api.Configuration;
import org.pentaho.hadoop.shim.common.authorization.HadoopAuthorizationService;
import org.pentaho.hadoop.shim.common.authorization.HasHadoopAuthorizationService;
import org.pentaho.hadoop.shim.spi.PigShim;

import java.net.URL;
import java.util.List;
import java.util.Properties;

public class DelegatingPigShim implements PigShim, HasHadoopAuthorizationService {
  private PigShim delegate;

  @Override
  public void setHadoopAuthorizationService( HadoopAuthorizationService hadoopAuthorizationService ) {
    delegate = hadoopAuthorizationService.getShim( PigShim.class );
  }

  @Override
  public ShimVersion getVersion() {
    return delegate.getVersion();
  }

  @Override
  public boolean isLocalExecutionSupported() {
    return delegate.isLocalExecutionSupported();
  }

  @Override
  public void configure( Properties properties, Configuration configuration ) {
    delegate.configure( properties, configuration );
  }

  @Override
  public String substituteParameters( URL pigScript, List<String> paramList ) throws Exception {
    return delegate.substituteParameters( pigScript, paramList );
  }

  @Override
  public int[] executeScript( String pigScript, ExecutionMode mode, Properties properties ) throws Exception {
    return delegate.executeScript( pigScript, mode, properties );
  }


}
