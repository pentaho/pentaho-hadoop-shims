package org.pentaho.big.data.api.cluster;

import org.pentaho.big.data.api.cluster.NamedCluster;

public interface INamedClusterSpecific {
    NamedCluster getNamedCluster();
    void setNamedCluster( NamedCluster namedCluster );
}
