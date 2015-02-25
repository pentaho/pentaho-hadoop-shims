package org.pentaho.hadoop.shim.hdp22;

import org.apache.pig.PigServer;
import org.apache.pig.tools.grunt.GruntParser;
import org.pentaho.hadoop.shim.common.CommonPigShim;

import java.io.IOException;
import java.io.StringReader;
import java.util.Properties;

public class PigShim extends CommonPigShim {
  @Override
  public int[] executeScript( String pigScript, ExecutionMode mode, Properties properties ) throws
            IOException, org.apache.pig.tools.pigscript.parser.ParseException {
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader( getClass().getClassLoader() );
    try {
      PigServer pigServer = new PigServer( getExecType( mode ), properties );
      GruntParser grunt = new GruntParser( new StringReader( pigScript ), pigServer );
      grunt.setInteractive( false );
      return grunt.parseStopOnError( false );
    } finally {
      Thread.currentThread().setContextClassLoader( cl );
    }
  }
}
