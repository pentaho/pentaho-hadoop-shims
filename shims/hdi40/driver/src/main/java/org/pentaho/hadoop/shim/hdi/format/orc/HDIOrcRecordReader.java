/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2021 by Hitachi Vantara : http://www.pentaho.com
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
package org.pentaho.hadoop.shim.hdi.format.orc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.pentaho.hadoop.shim.HadoopShim;
import org.pentaho.hadoop.shim.api.format.IOrcInputField;
import org.pentaho.hadoop.shim.common.format.orc.PentahoOrcRecordReader;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.List;

public class HDIOrcRecordReader extends PentahoOrcRecordReader {

  HDIOrcRecordReader( String fileName, Configuration conf,
                      List<? extends IOrcInputField> dialogInputFields, HadoopShim shim,
                      org.pentaho.hadoop.shim.api.internal.Configuration pentahoConf ) {
    super( fileName, dialogInputFields, getReader( fileName, conf, shim, pentahoConf ) );
  }

  static Reader getReader( String fileName, Configuration conf, HadoopShim shim,
                           org.pentaho.hadoop.shim.api.internal.Configuration pentahoConf ) {
    try {
      Path filePath = new Path( fileName );
      FileSystem fs = (FileSystem) shim.getFileSystem( pentahoConf ).getDelegate();
      if ( !fs.exists( filePath ) ) {
        throw new NoSuchFileException( fileName );
      }
      if ( fs.getFileStatus( filePath ).isDirectory() ) {
        PathFilter pathFilter = file -> file.getName().endsWith( ".orc" );

        FileStatus[] fileStatuses = fs.listStatus( filePath, pathFilter );
        if ( fileStatuses.length == 0 ) {
          throw new NoSuchFileException( fileName );
        }
        filePath = fileStatuses[0].getPath();
      }
      return OrcFile.createReader( filePath,
              OrcFile.readerOptions( conf ).filesystem( fs ) );
    } catch ( IOException e ) {
      throw new IllegalArgumentException( "Unable to read data from file " + fileName, e );
    }
  }
}