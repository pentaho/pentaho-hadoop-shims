package org.pentaho.hbase.shim.mapr31.authentication;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import javax.security.auth.login.LoginContext;

import org.apache.hadoop.conf.Configuration;
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
  public static final String PENTAHO_LOGIN_CONTEXT_UUID = "pentaho.login.context.uuid";
  private final LoginContext loginContext;
  private final String loginContextUuid;

  public HBaseKerberosShim( LoginContext loginContext ) {
    this.loginContext = loginContext;
    loginContextUuid = UUID.randomUUID().toString();
    HBaseKerberosUserProvider.setLoginContext( loginContextUuid, loginContext );
  }

  @Override
  public HBaseConnection getHBaseConnection() {
    return new DelegatingHBaseConnection( KerberosInvocationHandler.forObject( loginContext, new MapRHBaseConnection() {

      @Override
      public void configureConnection( Properties connProps, List<String> logMessages ) throws Exception {
        super.configureConnection( connProps, logMessages );
        setInfo( m_config );
      }
    }, new HashSet<Class<?>>( Arrays.<Class<?>> asList( HBaseShimInterface.class, HBaseConnectionInterface.class,
        HBaseBytesUtilShim.class ) ) ) );
  }
  
  @Override
  public void setInfo( Configuration configuration ) {
    super.setInfo( configuration );
    try {
      HadoopKerberosName.setConfiguration( configuration );
    } catch ( IOException e ) {
      throw new RuntimeException( e );
    }
    configuration.set( "hbase.client.userprovider.class", HBaseKerberosUserProvider.class.getCanonicalName() );
    configuration.set( PENTAHO_LOGIN_CONTEXT_UUID, loginContextUuid );
  }
}
