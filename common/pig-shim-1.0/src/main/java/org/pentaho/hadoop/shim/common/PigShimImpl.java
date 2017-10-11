/*! ******************************************************************************
 *
 * Pentaho Data Integration
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
package org.pentaho.hadoop.shim.common;

import org.apache.pig.PigServer;
import org.apache.pig.tools.grunt.GruntParser;

import java.io.IOException;
import java.io.StringReader;
import java.util.Properties;

/**
 * User: Dzmitry Stsiapanau Date: 11/18/2014 Time: 09:23
 */
public class PigShimImpl extends CommonPigShim {

  @Override
  public int[] executeScript( String pigScript, ExecutionMode mode, Properties properties )
    throws IOException, org.apache.pig.tools.pigscript.parser.ParseException {
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader( getClass().getClassLoader() );
    try {
      PigServer pigServer = new PigServer( getExecType( mode ), properties );
      GruntParser grunt = new GruntParser( new StringReader( pigScript ) );
      grunt.setInteractive( false );
      grunt.setParams( pigServer );
      return grunt.parseStopOnError( false );
    } finally {
      Thread.currentThread().setContextClassLoader( cl );
    }
  }

}
