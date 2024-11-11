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

package org.pentaho.hadoop.shim.common.format.parquet.delegate.apache;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.parquet.hadoop.Footer;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.ParquetRecordReader;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.hadoop.shim.ShimConfigsLoader;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.format.IParquetInputField;
import org.pentaho.hadoop.shim.api.format.IPentahoParquetInputFormat;
import org.pentaho.hadoop.shim.common.ConfigurationProxy;
import org.pentaho.hadoop.shim.common.format.HadoopFormatBase;
import org.pentaho.hadoop.shim.common.format.ReadFileFilter;
import org.pentaho.hadoop.shim.common.format.ReadFilesFilter;
import org.pentaho.hadoop.shim.common.format.S3NCredentialUtils;
import org.pentaho.hadoop.shim.common.format.parquet.ParquetInputFieldList;
import org.pentaho.hadoop.shim.common.format.parquet.PentahoInputSplitImpl;

import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static org.apache.hadoop.mapreduce.lib.input.FileInputFormat.setInputDirRecursive;
import static org.apache.hadoop.mapreduce.lib.input.FileInputFormat.setInputPathFilter;
import static org.apache.hadoop.mapreduce.lib.input.FileInputFormat.setInputPaths;
/**
 * Created by Vasilina_Terehova on 7/25/2017.
 */
public class PentahoApacheInputFormat extends HadoopFormatBase implements IPentahoParquetInputFormat {

  protected static final LogChannelInterface logger = LogChannel.GENERAL;

  private ParquetInputFormat<RowMetaAndData> nativeParquetInputFormat;
  protected Job job;

  public PentahoApacheInputFormat( NamedCluster namedCluster ) {
    logger.logBasic( "We are initializing parquet input format" );

    inClassloader( () -> {
      ConfigurationProxy conf = new ConfigurationProxy();

      if ( namedCluster != null ) {
        // if named cluster is not defined, no need to add cluster resource configs
        BiConsumer<InputStream, String> consumer = ( is, filename ) -> conf.addResource( is, filename );
        ShimConfigsLoader.addConfigsAsResources( namedCluster, consumer );
      }
      job = Job.getInstance( conf );

      nativeParquetInputFormat = new ParquetInputFormat<>();

      ParquetInputFormat.setReadSupportClass( job, PentahoParquetReadSupport.class );
      ParquetInputFormat.setTaskSideMetaData( job, false );
    } );
  }

  @Override public void setSchema( List<IParquetInputField> inputFields ) throws Exception {
    ParquetInputFieldList fieldList = new ParquetInputFieldList( inputFields );
    inClassloader( () -> job.getConfiguration().set( ParquetConverter.PARQUET_SCHEMA_CONF_KEY, fieldList.marshall() ) );
  }

  @Override public void setInputFile( String file ) throws Exception {
    inClassloader( () -> {
      S3NCredentialUtils util = new S3NCredentialUtils();
      util.applyS3CredentialsToHadoopConfigurationIfNecessary( file, job.getConfiguration() );
      Path filePath = new Path( S3NCredentialUtils.scrubFilePathIfNecessary( file ) );
      FileSystem fs = FileSystem.get( filePath.toUri(), job.getConfiguration() );
      filePath = fs.makeQualified( filePath );
      if ( !fs.exists( filePath ) ) {
        throw new NoSuchFileException( file );
      }
      if ( fs.getFileStatus( filePath ).isDirectory() ) { // directory
        setInputPaths( job, filePath );
        setInputDirRecursive( job, true );
      } else { // file
        setInputPaths( job, filePath.getParent() );
        setInputDirRecursive( job, false );
        setInputPathFilter( job, ReadFileFilter.class );
        job.getConfiguration().set( ReadFileFilter.FILTER_DIR, filePath.getParent().toString() );
        job.getConfiguration().set( ReadFileFilter.FILTER_FILE, filePath.toString() );
      }
    } );
  }

  @Override public void setInputFiles( String[] files ) throws Exception {
    inClassloader( () -> {

      boolean pathIsDir = false;
      String[] filePaths = new String[files.length];
      int i = 0;
      for ( String file : files ) {
        S3NCredentialUtils util = new S3NCredentialUtils();
        util.applyS3CredentialsToHadoopConfigurationIfNecessary( file, job.getConfiguration() );
        Path filePath = new Path( S3NCredentialUtils.scrubFilePathIfNecessary( file ) );
        FileSystem fs = FileSystem.get( filePath.toUri(), job.getConfiguration() );
        filePath = fs.makeQualified( filePath );
        if ( !fs.exists( filePath ) ) {
          throw new NoSuchFileException( file );
        }
        filePaths[i++] = filePath.toUri().toString();
        if ( fs.getFileStatus( filePath ).isDirectory() ) { // directory
          pathIsDir = true;
        }
      }
      if ( pathIsDir ) { // directory
        setInputPaths( job, String.join( ",", filePaths ) );
        setInputDirRecursive( job, true );
        job.getConfiguration().set( ReadFilesFilter.DIRECTORY, "true" );
        job.getConfiguration().set( ReadFilesFilter.FILE, "false" );
      } else { // file
        setInputPaths( job, String.join( ",", filePaths ) );
        setInputDirRecursive( job, false );
        setInputPathFilter( job, ReadFilesFilter.class );
        job.getConfiguration().set( ReadFilesFilter.FILE, "true" );
        job.getConfiguration().set( ReadFilesFilter.DIRECTORY, "false" );
      }
    } );
  }

  @Override @SuppressWarnings( "squid:CommentedOutCodeLine" ) public void setSplitSize( long blockSize )
      throws Exception {
    inClassloader( () ->
        /**
         * TODO Files splitting is temporary disabled. We need some UI checkbox for allow it, because some parquet files
         * can't be splitted by errors in previous implementation or other things. Parquet reports source of problem
         * only
         * to logs, not to exception. See CorruptDeltaByteArrays.requiresSequentialReads().
         *
         * mapr510 and mapr520 doesn't support SPLIT_FILES property
         */
        // ParquetInputFormat.setMaxInputSplitSize( job, blockSize );
        job.getConfiguration().setBoolean( ParquetInputFormat.SPLIT_FILES, false ) );
  }

  @Override public List<IPentahoInputSplit> getSplits() {
    return inClassloader( () -> {
      List<InputSplit> splits = nativeParquetInputFormat.getSplits( job );
      return splits.stream().map( PentahoInputSplitImpl::new ).collect( Collectors.toList() );
    } );
  }

  // for parquet not actual to point split
  @Override public IPentahoRecordReader createRecordReader( IPentahoInputSplit split ) throws Exception {
    return inClassloader( () -> {
      PentahoInputSplitImpl pentahoInputSplit = (PentahoInputSplitImpl) split;
      InputSplit inputSplit = pentahoInputSplit.getInputSplit();

      ReadSupport<RowMetaAndData> readSupport = new PentahoParquetReadSupport();

      ParquetRecordReader<RowMetaAndData>
          nativeRecordReader =
          new ParquetRecordReader<>( readSupport, ParquetInputFormat.getFilter( job.getConfiguration() ) );
      TaskAttemptContextImpl task = new TaskAttemptContextImpl( job.getConfiguration(), new TaskAttemptID() );
      nativeRecordReader.initialize( inputSplit, task );

      return new PentahoParquetRecordReader( nativeRecordReader );
    } );
  }

  @Override public List<IParquetInputField> readSchema( String file ) throws Exception {
    return inClassloader( () -> {
      Configuration conf = job.getConfiguration();
      S3NCredentialUtils util = new S3NCredentialUtils();
      util.applyS3CredentialsToHadoopConfigurationIfNecessary( file, conf );
      Path filePath = new Path( S3NCredentialUtils.scrubFilePathIfNecessary( file ) );
      FileSystem fs = FileSystem.get( filePath.toUri(), conf );
      FileStatus fileStatus = fs.getFileStatus( filePath );
      List<Footer> footers = ParquetFileReader.readFooters( conf, fileStatus, true );
      if ( footers.isEmpty() ) {
        return new ArrayList<>();
      } else {
        ParquetMetadata meta = footers.get( 0 ).getParquetMetadata();
        MessageType schema = meta.getFileMetaData().getSchema();
        return ParquetConverter.buildInputFields( schema );
      }
    } );
  }
}
