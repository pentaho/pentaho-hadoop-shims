/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2017 by Pentaho : http://www.pentaho.com
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
import org.pentaho.hadoop.shim.api.format.PentahoInputFormat;
import org.pentaho.hadoop.shim.api.format.PentahoOutputFormat;
import org.pentaho.hadoop.shim.api.process.Processable;
import org.pentaho.hadoop.shim.common.CommonFormatShim;
import org.pentaho.hadoop.shim.common.authorization.HadoopAuthorizationService;
import org.pentaho.hadoop.shim.common.authorization.HasHadoopAuthorizationService;
import org.pentaho.hadoop.shim.spi.FormatShim;

public class DelegatingFormatShim extends CommonFormatShim implements HasHadoopAuthorizationService, FormatShim,
  Processable {
  private FormatShim delegate;

  @Override
  public ShimVersion getVersion() {
    return delegate.getVersion();
  }

  @Override
  public void setHadoopAuthorizationService( HadoopAuthorizationService hadoopAuthorizationService ) {
    delegate = hadoopAuthorizationService.getShim( FormatShim.class );
  }

  @Override
  public PentahoInputFormat createInputFormat( FormatType type ) {
    return delegate.createInputFormat( type );
  }

  @Override public PentahoOutputFormat createOutputFormat( FormatType type ) {
    return delegate.createOutputFormat( type );
  }

  /*@Override
  public HBaseConnection getHBaseConnection() {
    return delegate.getHBaseConnection();
  }

  @Override
  public void setInfo( Configuration configuration ) {
    delegate.setInfo( configuration );
  }*/

  @Override public void process( org.pentaho.hadoop.shim.api.Configuration configuration ) { //TODO remove conf ?
    Processable processable;
    if ( Processable.class.isInstance( delegate ) ) {
      processable = (Processable) delegate;
      processable.process( configuration );
    }
  }
}
