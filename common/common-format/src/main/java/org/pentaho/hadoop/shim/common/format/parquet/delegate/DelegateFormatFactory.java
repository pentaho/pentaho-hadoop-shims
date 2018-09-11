package org.pentaho.hadoop.shim.common.format.parquet.delegate;

import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.common.format.parquet.delegate.apache.ApacheInputFormat;
import org.pentaho.hadoop.shim.common.format.parquet.delegate.apache.ApacheOutputFormat;
import org.pentaho.hadoop.shim.common.format.parquet.delegate.twitter.TwitterInputFormat;
import org.pentaho.hadoop.shim.common.format.parquet.delegate.twitter.TwitterOutputFormat;

public class DelegateFormatFactory {

    public static Object getInputFormatInstance( NamedCluster namedCluster ) throws Exception {

        String shimIdentifier = namedCluster.getShimIdentifier();
        if (shimIdentifier.startsWith("cdh") || (shimIdentifier.startsWith("mapr") && !shimIdentifier.equals( "mapr60" ))) {
            return new TwitterInputFormat( namedCluster );
        } else {
            return new ApacheInputFormat( namedCluster );
        }

    }

    public static Object getOutputFormatInstance( NamedCluster namedCluster ) throws Exception {

        String shimIdentifier = namedCluster.getShimIdentifier();
        if (shimIdentifier.startsWith("cdh") || (shimIdentifier.startsWith("mapr") && !shimIdentifier.equals( "mapr60" ))) {
            return new TwitterOutputFormat( );
        } else {
            return new ApacheOutputFormat( );
        }

    }
}
