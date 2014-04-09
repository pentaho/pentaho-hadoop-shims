package org.pentaho.hadoop.shim.mapr31.authorization;

import org.pentaho.di.core.auth.core.AuthenticationConsumptionException;

import com.mapr.fs.proto.Security.TicketAndKey;

public interface UserSpoofingHadoopAuthorizationCallable {
  public TicketAndKey call() throws AuthenticationConsumptionException;
}
