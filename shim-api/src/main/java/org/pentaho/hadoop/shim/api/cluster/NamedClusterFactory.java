package org.pentaho.hadoop.shim.api.cluster;

public interface NamedClusterFactory {
  NamedCluster getNamedCluster( String name );
}
