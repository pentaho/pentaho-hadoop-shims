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