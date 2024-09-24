/*! ******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
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
package org.pentaho.big.data.impl.shim.format;

import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.format.FormatService;
import org.pentaho.hadoop.shim.api.format.IPentahoInputFormat;
import org.pentaho.hadoop.shim.api.format.IPentahoOutputFormat;
import org.pentaho.hadoop.shim.spi.FormatShim;

import java.util.Objects;
import java.util.Optional;

import static org.pentaho.di.i18n.BaseMessages.getString;

public class FormatServiceImpl implements FormatService {

  private final FormatShim formatShim;

  private static final String PKG = "org.pentaho.big.data.impl.shim";
  private final String namedClusterName;

  @SuppressWarnings( "WeakerAccess" )
  public FormatServiceImpl( NamedCluster namedCluster, FormatShim formatShim ) {
    this.namedClusterName = namedCluster == null ? "undefined" : namedCluster.getName();
    this.formatShim = Objects.requireNonNull( formatShim );
  }

  @Override
  public <T extends IPentahoInputFormat> T createInputFormat( Class<T> type, NamedCluster namedCluster ) {
    return Optional.ofNullable(
      formatShim.createInputFormat( type, namedCluster ) )
      .orElseThrow( () -> new IllegalStateException(
        getString( PKG, "FormatService.FailedToFindFormat",
          type.getCanonicalName(), namedClusterName )
      ) );
  }

  @Override
  public <T extends IPentahoOutputFormat> T createOutputFormat( Class<T> type, NamedCluster namedCluster ) {
    return Optional.ofNullable(
      formatShim.createOutputFormat( type, namedCluster ) )
      .orElseThrow( () -> new IllegalStateException(
        getString( PKG, "FormatService.FailedToFindFormat",
          type.getCanonicalName(), namedClusterName )
      ) );
  }
}
