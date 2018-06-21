package org.pentaho.hadoop.shim;

/**
 * Created by Vasilina_Terehova on 10/10/2017.
 */
import org.apache.commons.vfs2.impl.DefaultFileSystemManager;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.hadoop.shim.spi.HadoopConfigurationProvider;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Vasilina_Terehova on 11/15/2016.
 */
public class HadoopConfigurationLocatorBundle implements HadoopConfigurationProvider {

  private static final Class<?> PKG = HadoopConfigurationLocatorBundle.class;
  String activeConfigurationId;
  /**
   * Currently known shim configurations
   */
  private Map<String, HadoopConfiguration> configurations = new HashMap<>();

  public HadoopConfigurationLocatorBundle(List<? extends HadoopConfiguration> hadoopConfigurations) {
    for (HadoopConfiguration hadoopConfiguration : hadoopConfigurations) {
      configurations.put(hadoopConfiguration.getIdentifier(), hadoopConfiguration);
      try {
        hadoopConfiguration.getHadoopShim().onLoad(hadoopConfiguration, new HadoopConfigurationFileSystemManager(this, new DefaultFileSystemManager()));
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    if (hadoopConfigurations.size() > 0) {
      activeConfigurationId = hadoopConfigurations.get(0).getIdentifier();
    }
  }

  @Override
  public boolean hasConfiguration(String id) {
    return configurations.containsKey(id);
  }

  @Override
  public List<? extends HadoopConfiguration> getConfigurations() {
    return new ArrayList<HadoopConfiguration>( configurations.values() );
  }

  @Override
  public HadoopConfiguration getConfiguration(String id) throws ConfigurationException {
    HadoopConfiguration config = configurations.get( id );
    if ( config == null ) {
      throw new ConfigurationException( BaseMessages.getString( PKG, "Error.UnknownHadoopConfiguration", id ) );
    }
    return config;
  }

  @Override
  public HadoopConfiguration getActiveConfiguration() throws ConfigurationException {
    if (activeConfigurationId == null) {
      return null;
    }
    return getConfiguration(activeConfigurationId);
  }
}
