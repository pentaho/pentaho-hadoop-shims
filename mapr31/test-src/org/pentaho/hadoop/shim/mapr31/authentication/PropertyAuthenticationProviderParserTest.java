package org.pentaho.hadoop.shim.mapr31.authentication;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Properties;

import org.junit.Test;
import org.pentaho.di.core.auth.KerberosAuthenticationProvider;
import org.pentaho.di.core.auth.UsernamePasswordAuthenticationProvider;
import org.pentaho.di.core.auth.core.AuthenticationManager;
import org.pentaho.di.core.encryption.Encr;
import org.pentaho.hadoop.shim.mapr31.authentication.PropertyAuthenticationProviderParser.AuthenticationProviderInstantiator;

public class PropertyAuthenticationProviderParserTest {
  @Test
  public void testHappyPath() {
    KerberosAuthenticationProvider kerberosAuthenticationProvider = new KerberosAuthenticationProvider();
    UsernamePasswordAuthenticationProvider usernamePasswordAuthenticationProvider =
        new UsernamePasswordAuthenticationProvider();
    Properties props = new Properties();
    props.setProperty( "authentication.provider.list", "kerby,notsokerby" );

    props.setProperty( "kerby.id", "kerby" );
    props.setProperty( "kerby.principal", "kerby@TEST.NET" );
    props.setProperty( "kerby.password", "kerbyPass" );
    props.setProperty( "kerby.keytabLocation", "/kerby/keytab" );
    props.setProperty( "kerby.class", kerberosAuthenticationProvider.getClass().getCanonicalName() );

    props.setProperty( "notsokerby.id", "notsokerby" );
    props.setProperty( "notsokerby.username", "mapruser" );
    props.setProperty( "notsokerby.password", "maprpass" );
    props.setProperty( "notsokerby.class", usernamePasswordAuthenticationProvider.getClass().getCanonicalName() );

    AuthenticationManager manager = mock( AuthenticationManager.class );
    AuthenticationProviderInstantiator instantiator = mock( AuthenticationProviderInstantiator.class );
    when( instantiator.instantiate( props.getProperty( "kerby.class" ) ) ).thenReturn( kerberosAuthenticationProvider );
    when( instantiator.instantiate( props.getProperty( "notsokerby.class" ) ) ).thenReturn(
        usernamePasswordAuthenticationProvider );
    PropertyAuthenticationProviderParser parser =
        new PropertyAuthenticationProviderParser( props, manager, instantiator );
    parser.process( "authentication.provider.list" );

    assertEquals( "kerby", kerberosAuthenticationProvider.getId() );
    assertEquals( "kerby@TEST.NET", kerberosAuthenticationProvider.getPrincipal() );
    assertEquals( "kerbyPass", kerberosAuthenticationProvider.getPassword() );
    assertEquals( "/kerby/keytab", kerberosAuthenticationProvider.getKeytabLocation() );

    assertEquals( "notsokerby", usernamePasswordAuthenticationProvider.getId() );
    assertEquals( "mapruser", usernamePasswordAuthenticationProvider.getUsername() );
    assertEquals( "maprpass", usernamePasswordAuthenticationProvider.getPassword() );

    verify( manager ).registerAuthenticationProvider( kerberosAuthenticationProvider );
    verify( manager ).registerAuthenticationProvider( usernamePasswordAuthenticationProvider );
  }

  @Test
  public void testObfuscatedPath() {
    KerberosAuthenticationProvider kerberosAuthenticationProvider = new KerberosAuthenticationProvider();
    UsernamePasswordAuthenticationProvider usernamePasswordAuthenticationProvider =
        new UsernamePasswordAuthenticationProvider();
    Properties props = new Properties();
    props.setProperty( "authentication.provider.list", "kerby,notsokerby" );

    props.setProperty( "kerby.id", Encr.encryptPasswordIfNotUsingVariables( "kerby" ) );
    props.setProperty( "kerby.principal", Encr.encryptPasswordIfNotUsingVariables( "kerby@TEST.NET" ) );
    props.setProperty( "kerby.password", Encr.encryptPasswordIfNotUsingVariables( "kerbyPass" ) );
    props.setProperty( "kerby.keytabLocation", Encr.encryptPasswordIfNotUsingVariables( "/kerby/keytab" ) );
    props.setProperty( "kerby.class", kerberosAuthenticationProvider.getClass().getCanonicalName() );

    props.setProperty( "notsokerby.id", Encr.encryptPasswordIfNotUsingVariables( "notsokerby" ) );
    props.setProperty( "notsokerby.username", Encr.encryptPasswordIfNotUsingVariables( "mapruser" ) );
    props.setProperty( "notsokerby.password", Encr.encryptPasswordIfNotUsingVariables( "maprpass" ) );
    props.setProperty( "notsokerby.class", usernamePasswordAuthenticationProvider.getClass().getCanonicalName() );

    AuthenticationManager manager = mock( AuthenticationManager.class );
    AuthenticationProviderInstantiator instantiator = mock( AuthenticationProviderInstantiator.class );
    when( instantiator.instantiate( props.getProperty( "kerby.class" ) ) ).thenReturn( kerberosAuthenticationProvider );
    when( instantiator.instantiate( props.getProperty( "notsokerby.class" ) ) ).thenReturn(
        usernamePasswordAuthenticationProvider );
    PropertyAuthenticationProviderParser parser =
        new PropertyAuthenticationProviderParser( props, manager, instantiator );
    parser.process( "authentication.provider.list" );

    assertEquals( "kerby", kerberosAuthenticationProvider.getId() );
    assertEquals( "kerby@TEST.NET", kerberosAuthenticationProvider.getPrincipal() );
    assertEquals( "kerbyPass", kerberosAuthenticationProvider.getPassword() );
    assertEquals( "/kerby/keytab", kerberosAuthenticationProvider.getKeytabLocation() );

    assertEquals( "notsokerby", usernamePasswordAuthenticationProvider.getId() );
    assertEquals( "mapruser", usernamePasswordAuthenticationProvider.getUsername() );
    assertEquals( "maprpass", usernamePasswordAuthenticationProvider.getPassword() );

    verify( manager ).registerAuthenticationProvider( kerberosAuthenticationProvider );
    verify( manager ).registerAuthenticationProvider( usernamePasswordAuthenticationProvider );
  }
}
