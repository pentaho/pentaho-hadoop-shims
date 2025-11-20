/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/

package org.pentaho.hadoop.shim.common.pvfs.conf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.pentaho.di.connections.ConnectionDetails;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.UUID;
import java.util.regex.Pattern;

public class SnwConf extends PvfsConf {
  private Pattern pathPattern = Pattern.compile( "pvfs://.+/@[~%]?[^/]*/?.*" );

  public SnwConf( ConnectionDetails details ) {
    super( details );
  }

  @Override public boolean supportsConnection() {
    return "snw".equals( details.getType() );
  }

  @Override public Path mapPath( Path pvfsPath ) {
    generateUnsupportedException();
    return null;
  }

  @Override public Path mapPath( Path pvfsPath, Path realFsPath ) {
    generateUnsupportedException();
    return null;
  }

  @Override public Configuration conf( Path pvfsPath ) {
    return new Configuration();
  }

  private void generateUnsupportedException() {
    throw new UnsupportedOperationException( "Use generateAlias method to process this URI" );
  }

  @Override
  public String generateAlias( String pvfsPath ) {
    validatePath( new Path( pvfsPath ) );
    if ( !pathPattern.matcher( pvfsPath ).matches() ) {
      throw new IllegalStateException( pvfsPath + " not supported by " + details.getClass().getName() );
    }
    java.nio.file.Path tempDir;
    try {
      tempDir = Files.createTempDirectory( "SnowflakeStaging" );
    } catch ( IOException e ) {
      throw new IllegalStateException( "Could not create a temporary work folder to hold the snowflake file" );
    }
    UUID uuid = UUID.randomUUID();
    return Paths.get( tempDir.toString() + java.io.File.separator + uuid.toString() ).toUri().toString();
  }
}
