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
