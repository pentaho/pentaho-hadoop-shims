/*******************************************************************************
 * Pentaho Big Data
 * <p>
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
 * <p>
 * ******************************************************************************
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 ******************************************************************************/

package org.pentaho.hadoop.shim.common.pig;

import org.apache.pig.PigServer;
import org.apache.pig.impl.PigContext;
import org.apache.pig.tools.grunt.GruntParser;
import org.pentaho.hadoop.shim.spi.PigShim;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Properties;

/**
 * User: Dzmitry Stsiapanau Date: 11/18/2014 Time: 09:23
 */
public class PigShimImpl extends CommonPigShim {

  @Override
  public int[] executeScript( String pigScript, PigShim.ExecutionMode mode, Properties properties )
    throws IOException, org.apache.pig.tools.pigscript.parser.ParseException {
    GruntParser grunt = null;
    PigContext pigContext = new PigContext( getExecType( mode ), properties );
    addExternalJarsToPigContext( pigContext );
    PigServer pigServer = new PigServer( pigContext );
    try {
      Constructor constructor = GruntParser.class.getConstructor( Reader.class, PigServer.class );
      grunt = (GruntParser)constructor.newInstance( new StringReader( pigScript ), pigServer );
    } catch ( Exception e ) {
      try {
        Constructor constructor = GruntParser.class.getConstructor( Reader.class );
        grunt = (GruntParser)constructor.newInstance( new StringReader( pigScript ) );
        Method method = grunt.getClass().getMethod("setParams", new Class[]{PigServer.class});
        method.invoke( grunt, pigServer );
      } catch ( Exception e1 ) {
      }
    }
    grunt.setInteractive( false );
    int[] retValues = grunt.parseStopOnError( false );
    return retValues;
  }
}
