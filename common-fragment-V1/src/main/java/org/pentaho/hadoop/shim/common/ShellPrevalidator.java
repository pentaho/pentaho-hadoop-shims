/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/

package org.pentaho.hadoop.shim.common;

import org.apache.commons.lang.SystemUtils;

import java.io.File;
import java.io.IOException;

/**
 * Created by Vasilina_Terehova on 10/3/2017.
 */
public class ShellPrevalidator {

  public static final String JAVA_VAR_HADOOP_HOME_DIR = "hadoop.home.dir";
  public static final String ENV_HADOOP_HOME = "HADOOP_HOME";

  static String checkHadoopHome() {
    String home = System.getProperty( JAVA_VAR_HADOOP_HOME_DIR );
    if ( home == null ) {
      home = System.getenv( ENV_HADOOP_HOME );
    }

    try {
      if ( home == null ) {
        throw new IOException( "HADOOP_HOME or hadoop.home.dir are not set." );
      }

      if ( home.startsWith( "\"" ) && home.endsWith( "\"" ) ) {
        home = home.substring( 1, home.length() - 1 );
      }

      File ioe = new File( home );
      if ( !ioe.isAbsolute() || !ioe.exists() || !ioe.isDirectory() ) {
        throw new IOException(
          "Hadoop home directory " + ioe + " does not exist, is not a directory, or is not an absolute path." );
      }

      home = ioe.getCanonicalPath();
    } catch ( IOException var2 ) {
      home = null;
    }

    return home;
  }

  public static boolean doesWinutilsFileExist() throws IOException {
    if ( isWindows() ) {
      String fullExeName = checkHadoopHome() + File.separator + "bin" + File.separator + "winutils.exe";
      if ( !doesFileExist( fullExeName ) ) {
        throw new IOException( "Could not locate executable " + fullExeName + " in the Hadoop binaries." );
      }
    }
    return true;
  }

  protected static boolean isWindows() {
    return SystemUtils.IS_OS_WINDOWS;
  }

  static boolean doesFileExist( String fullExeName ) {
    File exeFile = new File( fullExeName );
    return exeFile.exists();
  }


}
