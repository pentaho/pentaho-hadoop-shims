package org.pentaho.hbase.shim.hdp21;

import org.pentaho.hbase.shim.common.CommonHBaseConnection;
import org.pentaho.hbase.shim.common.wrapper.HBaseConnectionInterface;
import org.pentaho.hbase.shim.spi.IDeserializedBooleanComparator;
import org.pentaho.hbase.shim.spi.IDeserializedNumericComparator;

import java.util.Iterator;
import java.util.ServiceLoader;

public class HBaseConnectionImpl extends CommonHBaseConnection implements HBaseConnectionInterface {

  @Override
  public Class<?> getByteArrayComparableClass() throws ClassNotFoundException {
    return Class.forName( "org.apache.hadoop.hbase.filter.ByteArrayComparable" );
  }

  @Override
  public Class<?> getCompressionAlgorithmClass() throws ClassNotFoundException {
    return Class.forName( "org.apache.hadoop.hbase.io.compress.Compression.Algorithm" );
  }

  @Override
  public Class<?> getBloomTypeClass() throws ClassNotFoundException {
    return Class.forName( "org.apache.hadoop.hbase.regionserver.BloomType" );
  }

  @Override
  public Class<?> getDeserializedNumericComparatorClass() throws ClassNotFoundException {
    final Iterator<IDeserializedNumericComparator> providers =
      ServiceLoader.load( IDeserializedNumericComparator.class ).iterator();
    if ( providers.hasNext() ) {
      return providers.next().getClass();
    }
    return Class.forName( "org.pentaho.hbase.shim.common.DeserializedNumericComparator" );
  }

  @Override
  public Class<?> getDeserializedBooleanComparatorClass() throws ClassNotFoundException {
    final Iterator<IDeserializedBooleanComparator> providers =
      ServiceLoader.load( IDeserializedBooleanComparator.class ).iterator();
    if ( providers.hasNext() ) {
      return providers.next().getClass();
    }
    return Class.forName( "org.pentaho.hbase.shim.common.DeserializedBooleanComparator" );
  }
}
