/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2020 by Hitachi Vantara : http://www.pentaho.com
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
package org.pentaho.hadoop.shim.pvfs.conf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.pentaho.di.connections.ConnectionDetails;

import java.io.IOException;
import java.nio.file.Files;
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
    return "file:/" + tempDir.toString() + java.io.File.separator + uuid.toString();
  }
}
