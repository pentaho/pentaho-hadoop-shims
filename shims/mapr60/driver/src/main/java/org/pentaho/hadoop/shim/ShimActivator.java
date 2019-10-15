package org.pentaho.hadoop.shim;

import org.pentaho.hadoop.shim.common.osgi.jaas.JaasRealmsRegistrar;


public class ShimActivator implements BundleActivator {
  private static final String MAPR_LIB_PROPERTY = "mapr.library.flatclass";
  private static final String MAPR_CONFIG_FILE = "mapr.login.conf";

  @Override
  public void start( BundleContext bundleContext ) throws Exception {
    System.setProperty( MAPR_LIB_PROPERTY, "true" );
    JaasRealmsRegistrar registar = new JaasRealmsRegistrar( bundleContext );
    registar.setRealms( MAPR_CONFIG_FILE );
  }

  @Override
  public void stop( BundleContext bundleContext ) throws Exception {

  }
}
