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


package org.pentaho.hadoop.shim;

import org.pentaho.hadoop.shim.api.core.ShimIdentifierInterface;
import org.pentaho.hadoop.shim.api.internal.ShimIdentifier;
import org.pentaho.hadoop.shim.common.ConfigurationProxyV2;
import org.pentaho.hadoop.shim.common.HadoopShimImpl;
import org.pentaho.hadoop.shim.common.format.S3NCredentialUtils;

public class HadoopShim extends HadoopShimImpl {

  private static final String ID = "apache";
  private static final String VENDOR = "Apache";
  private static final String VERSION = "3.4";
  private static final ShimIdentifierInterface.ShimType TYPE = ShimIdentifierInterface.ShimType.COMMUNITY;
  private static final ShimIdentifier SHIM_IDENTIFIER = new ShimIdentifier( ID, VENDOR, VERSION, TYPE );

  public HadoopShim() {
    super();
    S3NCredentialUtils.setS3nIsSupported( false );
  }

  @Override
  public Class[] getHbaseDependencyClasses() {
    return new Class[0];
  }

  @Override
  public org.pentaho.hadoop.shim.api.internal.Configuration createConfiguration( String namedClusterConfigId ) {
    ConfigurationProxyV2 conf = (ConfigurationProxyV2) super.createConfiguration( namedClusterConfigId );
    conf.getJob().getConfiguration().setRestrictSystemProperties( false );
    return conf;
  }

  @Override
  public ShimIdentifier getShimIdentifier() {
    return SHIM_IDENTIFIER;
  }

}
