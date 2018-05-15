package org.pentaho.hadoop.shim;

import org.pentaho.hadoop.shim.api.HadoopConfigurationInterface;
import org.pentaho.hadoop.shim.api.HasConfiguration;

/**
 * Created by Vasilina_Terehova on 10/10/2017.
 */
public class HasConfigurationImpl implements HasConfiguration {
  HadoopConfiguration hadoopConfiguration;

  public HasConfigurationImpl( HadoopConfigurationInterface hadoopConfiguration ) {
    this.hadoopConfiguration = (HadoopConfiguration) hadoopConfiguration;
  }

  @Override
  public HadoopConfiguration getHadoopConfiguration() {
    return hadoopConfiguration;
  }
}
