/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
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
