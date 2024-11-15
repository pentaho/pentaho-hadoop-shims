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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.pentaho.hadoop.shim.api.format.IOrcOutputField;
import org.pentaho.hadoop.shim.common.format.S3NCredentialUtils;
import org.pentaho.hadoop.shim.common.format.orc.PentahoOrcRecordWriter;

import java.io.IOException;
import java.util.List;

public class HDIOrcRecordWriter extends PentahoOrcRecordWriter {

  public HDIOrcRecordWriter( List<? extends IOrcOutputField> fields, TypeDescription schema, String filePath,
                             Configuration conf, FileSystem fileSystem ) {
    super( fields, schema, filePath, conf );
    try {
      Path outputFile = new Path( S3NCredentialUtils.scrubFilePathIfNecessary( filePath ) );
      writer = OrcFile.createWriter( outputFile,
              OrcFile.writerOptions( conf ).fileSystem( fileSystem )
                      .setSchema( schema ) );
      batch = schema.createRowBatch();
    } catch ( IOException e ) {
      logger.error( e );
    }
  }
}
