/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/

package org.pentaho.hadoop.shim;

import org.pentaho.hadoop.shim.common.ConfigurationProxyV2;
import org.pentaho.hadoop.shim.common.HadoopShimImpl;
import org.pentaho.hadoop.shim.common.format.S3NCredentialUtils;

public class HadoopShim extends HadoopShimImpl {

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

}
