/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2015 by Pentaho : http://www.pentaho.com
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

package org.pentaho.hadoop.shim.api;

import java.io.InputStream;
import java.io.StringWriter;

import org.w3c.dom.Document;
import org.w3c.dom.Node;

import org.apache.commons.io.IOUtils;
import org.pentaho.di.core.namedconfig.INamedConfigurationManager;
import org.pentaho.di.core.namedconfig.NamedConfigurationManager;
import org.pentaho.di.core.namedconfig.model.NamedConfiguration;
import org.pentaho.di.core.xml.XMLHandler;

public class NamedConfigurationLoader {

  public static void load( Class<?> clazz ) {
    INamedConfigurationManager ncm = NamedConfigurationManager.getInstance();
    ncm.setActiveShimClass( clazz.getName() );

    NamedConfiguration configuration = new NamedConfiguration( );
    try {
      InputStream xmlIn = clazz.getResourceAsStream( "/named-cluster-configuration.xml" );
      StringWriter writer = new StringWriter();
      IOUtils.copy( xmlIn, writer, "UTF-8" );
      String xml = writer.toString();
      configuration = new NamedConfiguration( xml );
      configuration.setSubType( clazz.getName() );
    } catch ( Exception e ) {
      configuration.setName( HasNamedConfiguration.INVALID_CONFIG );
      configuration.setType( HasNamedConfiguration.TYPE );
      configuration.setSubType( clazz.getName() );
    }
    ncm.addConfigurationTemplate( configuration );      
  }
  
}
