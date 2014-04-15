package org.pentaho.hbase.shim.mapr31.authentication;

import java.io.IOException;
import java.security.Principal;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;

import org.apache.hadoop.hbase.security.User.SecureHadoopUser;
import org.apache.hadoop.security.HadoopKerberosName;
import org.apache.hadoop.security.UserGroupInformation;

public class HBaseKerberosUser extends SecureHadoopUser {
  private final LoginContext loginContext;

  public HBaseKerberosUser( UserGroupInformation ugi, LoginContext loginContext ) {
    super( ugi );
    this.loginContext = loginContext;
  }

  @Override
  public String getName() {
    List<Principal> principals = new ArrayList<Principal>( loginContext.getSubject().getPrincipals() );
    if ( principals.size() != 1 ) {
      throw new RuntimeException( "Expected 1 principal, found " + principals.size() );
    }
    return principals.get( 0 ).getName();
  }

  @Override
  public String getShortName() {
    try {
      return new HadoopKerberosName( getName() ).getShortName();
    } catch ( IOException e ) {
      throw new RuntimeException( e );
    }
  }

  @Override
  public <T> T runAs( PrivilegedAction<T> action ) {
    return Subject.doAs( loginContext.getSubject(), action );
  }

  @Override
  public <T> T runAs( PrivilegedExceptionAction<T> action ) throws IOException, InterruptedException {
    try {
      return Subject.doAs( loginContext.getSubject(), action );
    } catch ( PrivilegedActionException e ) {
      throw new IOException( e );
    }
  }
}
