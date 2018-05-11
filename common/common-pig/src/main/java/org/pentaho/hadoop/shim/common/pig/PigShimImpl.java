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
//$import java.io.StringReader;
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
    //#if shim_type=="CDH"
    //$grunt = new GruntParser( new StringReader( pigScript ) );
    //$grunt.setParams( pigServer );
    //#else
    //$grunt = new GruntParser( new StringReader( pigScript ), pigServer );
    //#endif
    grunt.setInteractive( false );
    return grunt.parseStopOnError( false );
  }
}
