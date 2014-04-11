package org.pentaho.hbase.shim.mapr31.authentication;

import javax.security.auth.login.LoginException;

import org.pentaho.di.core.auth.AuthenticationConsumerPlugin;
import org.pentaho.di.core.auth.AuthenticationConsumerType;
import org.pentaho.di.core.auth.KerberosAuthenticationProvider;
import org.pentaho.di.core.auth.core.AuthenticationConsumer;
import org.pentaho.di.core.auth.core.AuthenticationConsumptionException;
import org.pentaho.hadoop.shim.mapr31.authentication.KerberosConsumerUtil;
import org.pentaho.hbase.shim.mapr31.wrapper.HBaseShimInterface;

public class HBaseKerberosConsumer implements
    AuthenticationConsumer<HBaseShimInterface, KerberosAuthenticationProvider> {
  @AuthenticationConsumerPlugin( id = "HBaseKerberosConsumer", name = "HBaseKerberosConsumer" )
  public static class MapRSuperUserKerberosConsumerType implements AuthenticationConsumerType {

    @Override
    public String getDisplayName() {
      return "HBaseKerberosConsumer";
    }

    @Override
    public Class<? extends AuthenticationConsumer<?, ?>> getConsumerClass() {
      return HBaseKerberosConsumer.class;
    }
  }

  private final KerberosConsumerUtil kerberosUtil;

  public HBaseKerberosConsumer( Void client ) {
    this.kerberosUtil = new KerberosConsumerUtil();
  }

  @Override
  public HBaseShimInterface consume( final KerberosAuthenticationProvider authenticationProvider )
    throws AuthenticationConsumptionException {
    try {
      return new HBaseKerberosShim( kerberosUtil.createLoginContext( authenticationProvider ) );
    } catch ( LoginException e ) {
      throw new AuthenticationConsumptionException( e );
    }
  }
}
