package org.pentaho.hbase.shim.mapr31.authentication;

import java.io.IOException;

import javax.security.auth.login.LoginContext;

import org.apache.hadoop.hbase.client.UserProvider;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.security.UserGroupInformation;

public class HBaseKerberosUserProvider extends UserProvider {
  public static LoginContext loginContext;

  @Override
  public User getCurrent() throws IOException {
    return new HBaseKerberosUser( UserGroupInformation.getCurrentUser(), loginContext );
  }
}
