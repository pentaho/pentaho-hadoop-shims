package org.pentaho.hadoop.shim.common.format.parquet.delegate;

import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.common.format.parquet.delegate.apache.PentahoApacheInputFormat;
import org.pentaho.hadoop.shim.common.format.parquet.delegate.apache.PentahoApacheOutputFormat;
import org.pentaho.hadoop.shim.common.format.parquet.delegate.twitter.PentahoTwitterInputFormat;
import org.pentaho.hadoop.shim.common.format.parquet.delegate.twitter.PentahoTwitterOutputFormat;

public class DelegateFormatFactory {

    public static Object getInputFormatInstance( NamedCluster namedCluster ) throws Exception {

        String shimIdentifier = namedCluster.getShimIdentifier();
        if ((shimIdentifier.startsWith("cdh") && !shimIdentifier.startsWith("cdh6")) || (shimIdentifier.startsWith("mapr") && !shimIdentifier.equals( "mapr60" ))) {
            return new PentahoTwitterInputFormat( namedCluster );
        } else {
            return new PentahoApacheInputFormat( namedCluster );
        }
    }

    public static Object getOutputFormatInstance( NamedCluster namedCluster ) throws Exception {

        String shimIdentifier = namedCluster.getShimIdentifier();
        if ((shimIdentifier.startsWith("cdh") && !shimIdentifier.startsWith("cdh6")) || (shimIdentifier.startsWith("mapr") && !shimIdentifier.equals( "mapr60" ))) {
            return new PentahoTwitterOutputFormat( );
        } else {
            return new PentahoApacheOutputFormat( );
        }
    }
}
