package org.pentaho.hadoop.shim.mapr31.authorization;

import javax.security.auth.login.LoginContext;

import org.pentaho.di.core.auth.core.AuthenticationConsumptionException;

import com.mapr.fs.proto.Security.TicketAndKey;

public interface UserSpoofingHadoopAuthorizationCallable {
  public TicketAndKey call() throws AuthenticationConsumptionException;
  
  public LoginContext getLoginContext();
}
