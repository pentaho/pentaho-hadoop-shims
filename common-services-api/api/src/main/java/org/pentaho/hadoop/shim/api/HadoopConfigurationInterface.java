package org.pentaho.hadoop.shim.api;

import org.pentaho.hadoop.shim.ConfigurationException;
import org.pentaho.hadoop.shim.spi.HadoopShim;
import org.pentaho.hadoop.shim.spi.PentahoHadoopShim;
import org.pentaho.hadoop.shim.spi.PigShim;
import org.pentaho.hadoop.shim.spi.SnappyShim;
import org.pentaho.hadoop.shim.spi.SqoopShim;
import org.pentaho.hbase.shim.spi.HBaseShim;

import java.util.List;
import java.util.Properties;

/**
 * Created by Vasilina_Terehova on 10/10/2017.
 */
public interface HadoopConfigurationInterface {
  String getIdentifier();

  String getName();

  HadoopShim getHadoopShim();

  SqoopShim getSqoopShim() throws ConfigurationException;

  PigShim getPigShim() throws ConfigurationException;

  SnappyShim getSnappyShim() throws ConfigurationException;

  <T extends PentahoHadoopShim> T getShim( Class<T> shimType ) throws ConfigurationException;

  HBaseShim getHBaseShim() throws ConfigurationException;

  void process( Configuration configuration );

  List<PentahoHadoopShim> getAvailableShims();

  Properties getConfigProperties();
}
