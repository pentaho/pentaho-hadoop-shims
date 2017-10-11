/*******************************************************************************
*
* Pentaho Big Data
*
* Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
*
*******************************************************************************
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with
* the License. You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*
******************************************************************************/

package org.pentaho.hadoop.shim.common.authentication;

import org.apache.commons.lang.ClassUtils;
import org.apache.log4j.Logger;
import org.pentaho.di.core.auth.core.AuthenticationManager;
import org.pentaho.di.core.auth.core.AuthenticationProvider;
import org.pentaho.di.core.encryption.Encr;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Properties;

public class PropertyAuthenticationProviderParser {
  public static interface AuthenticationProviderInstantiator {
    public AuthenticationProvider instantiate( String canonicalName );
  }

  private static final Logger logger = Logger.getLogger( PropertyAuthenticationProviderParser.class );
  private final Properties properties;
  private final AuthenticationManager manager;
  private final AuthenticationProviderInstantiator authenticationProviderInstantiator;

  public PropertyAuthenticationProviderParser( Properties properties, AuthenticationManager manager ) {
    this( properties, manager, new AuthenticationProviderInstantiator() {

      @Override
      public AuthenticationProvider instantiate( String canonicalName ) {
        Class<?> clazz = null;
        try {
          clazz = Class.forName( canonicalName );
        } catch ( ClassNotFoundException e ) {
          logger.warn( "Cannot locate class " + canonicalName + ", provider will not be processed.", e );
        }
        if ( clazz != null ) {
          try {
            return (AuthenticationProvider) clazz.newInstance();
          } catch ( Exception e ) {
            logger.warn( "Cannot instantiate class " + canonicalName + ", provider will not be processed.", e );
          }
        }
        return null;
      }
    } );
  }

  public PropertyAuthenticationProviderParser( Properties properties, AuthenticationManager manager,
                                               AuthenticationProviderInstantiator authenticationProviderInstantiator ) {
    this.properties = properties;
    this.manager = manager;
    this.authenticationProviderInstantiator = authenticationProviderInstantiator;
  }

  public void process( String providerListProperty ) {
    if ( properties.containsKey( providerListProperty ) ) {
      for ( String prefix : properties.getProperty( providerListProperty ).split( "," ) ) {
        prefix = prefix.trim();
        if ( prefix.length() > 0 ) {
          processPrefix( prefix );
        }
      }
    }
  }

  private void processPrefix( String prefix ) {
    AuthenticationProvider provider =
        authenticationProviderInstantiator.instantiate( properties.getProperty( prefix + ".class" ) );
    if ( provider != null ) {
      for ( Method method : provider.getClass().getMethods() ) {
        if ( method.getName().startsWith( "set" ) && method.getName().length() >= 4
            && method.getParameterTypes().length == 1 ) {
          String propName = prefix + "." + method.getName().substring( 3, 4 ).toLowerCase();
          if ( method.getName().length() > 4 ) {
            propName += method.getName().substring( 4 );
          }
          if ( properties.containsKey( propName ) ) {
            String strValue = Encr.decryptPasswordOptionallyEncrypted( properties.getProperty( propName ) );
            Object actualValue = null;
            Class<?> argType = method.getParameterTypes()[0];
            if ( argType.isPrimitive() ) {
              argType = ClassUtils.primitiveToWrapper( argType );
            }
            if ( argType == String.class ) {
              actualValue = strValue;
            } else {
              Method valueOf = null;
              try {
                valueOf = argType.getMethod( "valueOf", String.class );
              } catch ( NoSuchMethodException e1 ) {
                logger.warn( "Unable to find valueOf method on " + argType.getClass().getCanonicalName() );
              }
              if ( valueOf != null && Modifier.isStatic( valueOf.getModifiers() ) ) {
                try {
                  actualValue = valueOf.invoke( null, strValue );
                } catch ( Exception e ) {
                  logger.warn( "Unable to convert string property " + propName + "(" + strValue + ") to "
                      + argType.getClass().getCanonicalName() + " to invoke setter " + method.getName(), e );
                }
              } else {
                logger.warn( "Could not find method to convert " + propName + "(" + strValue + ") to "
                    + argType.getClass().getCanonicalName()
                    + " (currently only primitives and their wrappers are supported)" );
              }
            }
            if ( actualValue != null ) {
              try {
                method.invoke( provider, actualValue );
              } catch ( Exception e ) {
                Throwable cause = e;
                if ( e instanceof InvocationTargetException ) {
                  cause = e.getCause();
                }
                logger.warn( "Error invoking setter " + method.toString() + " with property " + propName + "("
                    + strValue + ")", cause );
              }
            }
          }

        }
      }
      manager.registerAuthenticationProvider( provider );
    }
  }
}
