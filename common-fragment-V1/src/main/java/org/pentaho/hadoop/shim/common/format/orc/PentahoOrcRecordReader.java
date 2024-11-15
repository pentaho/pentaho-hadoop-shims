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


package org.pentaho.hadoop.shim.common.format.orc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.hadoop.shim.api.format.IOrcInputField;
import org.pentaho.hadoop.shim.api.format.IOrcMetaData;
import org.pentaho.hadoop.shim.api.format.IPentahoInputFormat;
import org.pentaho.hadoop.shim.common.format.S3NCredentialUtils;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by tkafalas on 11/7/2017.
 */
public class PentahoOrcRecordReader implements IPentahoInputFormat.IPentahoRecordReader {
  protected static Logger logger = LogManager.getLogger( PentahoOrcRecordReader.class );
  protected List<? extends IOrcInputField> dialogInputFields;  //Comes from Dialog
  protected List<? extends IOrcInputField> orcInputFields;  //Comes from OrcFile combined with custom metadata
  protected VectorizedRowBatch batch;
  protected RecordReader recordReader;
  protected int currentBatchRow;
  protected TypeDescription typeDescription;
  protected Map<String, Integer> schemaToOrcSubcripts;
  protected OrcConverter orcConverter = new OrcConverter();

  protected PentahoOrcRecordReader( String fileName, Configuration conf,
                          List<? extends IOrcInputField> dialogInputFields ) {
    this.dialogInputFields = dialogInputFields;

    Reader reader = getReader( fileName, conf );
    readRows( fileName, reader );
  }

  protected PentahoOrcRecordReader( String fileName, List<? extends IOrcInputField> dialogInputFields, Reader reader ) {
    this.dialogInputFields = dialogInputFields;
    readRows( fileName, reader );
  }

  private void readRows( String fileName, Reader reader ) {
    try {
      recordReader = reader.rows();
    } catch ( IOException e ) {
      throw new IllegalArgumentException( "Unable to get record reader for file " + fileName, e );
    }
    typeDescription = reader.getSchema();
    OrcSchemaConverter orcSchemaConverter = new OrcSchemaConverter();
    orcInputFields = orcSchemaConverter.buildInputFields( typeDescription );
    IOrcMetaData.Reader orcMetaDataReader = new OrcMetaDataReader( reader );
    orcMetaDataReader.read( orcInputFields );
    batch = typeDescription.createRowBatch();

    //Create a map of orc fields to meta columns
    Map<String, Integer> orcColumnNumberMap = new HashMap<>();
    int orcFieldNumber = 0;
    for ( String orcFieldName : typeDescription.getFieldNames() ) {
      orcColumnNumberMap.put( orcFieldName, orcFieldNumber++ );
    }

    //Create a map of input fields to Orc Column numbers
    schemaToOrcSubcripts = new HashMap<>();
    for ( IOrcInputField inputField : dialogInputFields ) {
      if ( inputField != null ) {
        Integer colNumber = orcColumnNumberMap.get( inputField.getFormatFieldName() );
        if ( colNumber == null ) {
          throw new IllegalArgumentException(
                  "Column " + inputField.getFormatFieldName()
                          + " does not exist in the ORC file.  Please use the getFields button" );
        } else {
          schemaToOrcSubcripts.put( inputField.getPentahoFieldName(), colNumber );
        }
      }
    }

    try {
      setNextBatch();
    } catch ( IOException e ) {
      throw new IllegalArgumentException( "No rows to read in " + fileName, e );
    }
  }

  static Reader getReader( String fileName, Configuration conf ) {

    try {
      S3NCredentialUtils util = new S3NCredentialUtils();
      util.applyS3CredentialsToHadoopConfigurationIfNecessary( fileName, conf );
      Path filePath = new Path( S3NCredentialUtils.scrubFilePathIfNecessary( fileName ) );
      FileSystem fs = FileSystem.get( filePath.toUri(), conf );
      if ( !fs.exists( filePath ) ) {
        throw new NoSuchFileException( fileName );
      }
      if ( fs.getFileStatus( filePath ).isDirectory() ) {
        PathFilter pathFilter = file -> file.getName().endsWith( ".orc" );

        FileStatus[] fileStatuses = fs.listStatus( filePath, pathFilter );
        if ( fileStatuses.length == 0 ) {
          throw new NoSuchFileException( fileName );
        }
        filePath = fileStatuses[ 0 ].getPath();
      }
      return OrcFile.createReader( filePath,
        OrcFile.readerOptions( conf ).filesystem( fs ) );
    } catch ( IOException e ) {
      throw new IllegalArgumentException( "Unable to read data from file " + fileName, e );
    }
  }


  protected boolean setNextBatch() throws IOException {
    currentBatchRow = 0;
    return recordReader.nextBatch( batch );
  }

  @Override public void close() throws IOException {
    recordReader.close();
  }


  @Override public Iterator<RowMetaAndData> iterator() {
    return new Iterator<RowMetaAndData>() {

      @Override public boolean hasNext() {
        if ( currentBatchRow < batch.size ) {
          return true;
        }
        try {
          return setNextBatch();
        } catch ( IOException e ) {
          logger.error( e.getMessage(), e );
          return false;
        }
      }

      @Override public RowMetaAndData next() {
        RowMetaAndData rowMeta =
          orcConverter.convertFromOrc( batch, currentBatchRow, dialogInputFields, typeDescription, schemaToOrcSubcripts,
            orcInputFields );
        currentBatchRow++;
        return rowMeta;
      }
    };
  }

}
