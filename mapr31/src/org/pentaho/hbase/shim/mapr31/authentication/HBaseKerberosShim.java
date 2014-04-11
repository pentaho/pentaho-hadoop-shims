package org.pentaho.hbase.shim.mapr31.authentication;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;

import javax.security.auth.login.LoginContext;

import org.apache.hadoop.security.HadoopKerberosName;
import org.pentaho.hadoop.shim.mapr31.authorization.KerberosInvocationHandler;
import org.pentaho.hadoop.shim.mapr31.delegatingShims.DelegatingHBaseConnection;
import org.pentaho.hbase.shim.mapr31.MapRHBaseConnection;
import org.pentaho.hbase.shim.mapr31.MapRHBaseShim;
import org.pentaho.hbase.shim.mapr31.wrapper.HBaseConnectionInterface;
import org.pentaho.hbase.shim.mapr31.wrapper.HBaseShimInterface;
import org.pentaho.hbase.shim.spi.HBaseBytesUtilShim;
import org.pentaho.hbase.shim.spi.HBaseConnection;

public class HBaseKerberosShim extends MapRHBaseShim implements HBaseShimInterface {
  private final LoginContext loginContext;

  public HBaseKerberosShim( LoginContext loginContext ) {
    this.loginContext = loginContext;
    HBaseKerberosUserProvider.loginContext = loginContext;
  }

  @Override
  public HBaseConnection getHBaseConnection() {
    return new DelegatingHBaseConnection( KerberosInvocationHandler.forObject( loginContext, new MapRHBaseConnection() {

      @Override
      public void configureConnection( Properties connProps, List<String> logMessages ) throws Exception {
        super.configureConnection( connProps, logMessages );
        
        m_config.set( "hbase.client.userprovider.class", HBaseKerberosUserProvider.class.getCanonicalName() );
        HadoopKerberosName.setConfiguration( m_config );
      }
    }, new HashSet<Class<?>>( Arrays.<Class<?>> asList( HBaseShimInterface.class, HBaseConnectionInterface.class,
        HBaseBytesUtilShim.class ) ) ) );
  }
}
