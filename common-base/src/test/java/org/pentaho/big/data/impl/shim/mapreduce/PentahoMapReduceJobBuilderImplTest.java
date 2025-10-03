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


package org.pentaho.big.data.impl.shim.mapreduce;

import org.apache.commons.vfs2.FileObject;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.Credentials;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import org.pentaho.bigdata.api.mapreduce.PentahoMapReduceOutputStepMetaInterface;
import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.DBCache;
import org.pentaho.di.core.KettleClientEnvironment;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleFileException;
import org.pentaho.di.core.logging.ChannelLogTable;
import org.pentaho.di.core.logging.KettleLogStore;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.core.logging.MetricsLogTable;
import org.pentaho.di.core.logging.PerformanceLogTable;
import org.pentaho.di.core.logging.StepLogTable;
import org.pentaho.di.core.logging.TransLogTable;
import org.pentaho.di.core.plugins.PluginInterface;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.HasDatabasesInterface;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransConfiguration;
import org.pentaho.di.trans.TransExecutionConfiguration;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.hadoop.shim.api.ConfigurationException;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.internal.Configuration;
import org.pentaho.hadoop.shim.api.internal.DistributedCacheUtil;
import org.pentaho.hadoop.shim.api.internal.fs.FileSystem;
import org.pentaho.hadoop.shim.api.internal.fs.Path;
import org.pentaho.hadoop.shim.spi.HadoopShim;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/**
 * Created by bryan on 1/12/16.
 */
@FixMethodOrder( MethodSorters.NAME_ASCENDING )
@RunWith( MockitoJUnitRunner.class )
public class PentahoMapReduceJobBuilderImplTest {

  @Mock private NamedCluster namedCluster;
  @Mock private LogChannelInterface logChannelInterface;
  @Mock private HadoopShim hadoopShim;
  @Mock private PluginInterface pluginInterface;
  @Mock private Properties pmrProperties;
  @Mock private TransMeta transMeta;
  @Mock private PentahoMapReduceJobBuilderImpl.TransFactory transFactory;
  @Mock private PentahoMapReduceJobBuilderImpl.PMRArchiveGetter pmrArchiveGetter;
  @Mock private FileObject vfsPluginDirectory;

  @Rule
  public ExpectedException expectedEx = ExpectedException.none();

  private PentahoMapReduceJobBuilderImpl pentahoMapReduceJobBuilder;
  private VariableSpace variableSpace = new Variables();
  private List<TransformationVisitorService> visitorServices = new ArrayList<>();
  private String transXml;

  @Before
  public void setup() throws Exception {
    KettleClientEnvironment.init();
    KettleLogStore.init();
    DBCache.getInstance().setInactive();

    TransConfiguration transConfig = new TransConfiguration( new TransMeta(), new TransExecutionConfiguration() );
    transXml = TransConfiguration.fromXML( transConfig.getXML() ).getXML();

    visitorServices.add( new MockVisitorService() );

    pentahoMapReduceJobBuilder =
      new PentahoMapReduceJobBuilderImpl( namedCluster, hadoopShim, logChannelInterface, variableSpace,
        pluginInterface, vfsPluginDirectory, pmrProperties, transFactory, pmrArchiveGetter, visitorServices );
  }

  @Test
  public void testGetHadoopWritableCompatibleClassName() {
    ValueMetaInterface valueMetaInterface = mock( ValueMetaInterface.class );
    when( hadoopShim.getHadoopWritableCompatibleClass( valueMetaInterface ) ).thenReturn( null, (Class) String.class );
    assertNull( pentahoMapReduceJobBuilder.getHadoopWritableCompatibleClassName( valueMetaInterface ) );
    assertEquals( String.class.getCanonicalName(),
      pentahoMapReduceJobBuilder.getHadoopWritableCompatibleClassName( valueMetaInterface ) );
  }

  @Test( expected = KettleException.class )
  public void testVerifyTransMetaEmptyInputStepName() throws KettleException {
    try {
      pentahoMapReduceJobBuilder.verifyTransMeta( transMeta, null, null );
    } catch ( KettleException e ) {
      assertEquals( BaseMessages.getString( PentahoMapReduceJobBuilderImpl.PKG,
        PentahoMapReduceJobBuilderImpl.PENTAHO_MAP_REDUCE_JOB_BUILDER_IMPL_INPUT_STEP_NOT_SPECIFIED ),
        e.getMessage().trim() );
      throw e;
    }
  }

  @Test( expected = KettleException.class )
  public void testVerifyTransMetaCanFindInputStep() throws KettleException {
    String inputStepName = "inputStepName";
    when( transMeta.findStep( inputStepName ) ).thenReturn( null );
    try {
      pentahoMapReduceJobBuilder.verifyTransMeta( transMeta, inputStepName, null );
    } catch ( KettleException e ) {
      assertEquals( BaseMessages.getString( PentahoMapReduceJobBuilderImpl.PKG,
        PentahoMapReduceJobBuilderImpl.PENTAHO_MAP_REDUCE_JOB_BUILDER_IMPL_INPUT_STEP_NOT_FOUND, inputStepName ),
        e.getMessage().trim() );
      throw e;
    }
  }

  @Test( expected = KettleException.class )
  public void testVerifyTransMetaNoKeyOrdinal() throws KettleException {
    String inputStepName = "inputStepName";
    StepMeta inputStepMeta = mock( StepMeta.class );
    when( transMeta.findStep( inputStepName ) ).thenReturn( inputStepMeta );
    RowMetaInterface rowMetaInterface = mock( RowMetaInterface.class );
    when( rowMetaInterface.getFieldNames() ).thenReturn( new String[] {} );
    when( transMeta.getStepFields( inputStepMeta ) ).thenReturn( rowMetaInterface );
    try {
      pentahoMapReduceJobBuilder.verifyTransMeta( transMeta, inputStepName, null );
    } catch ( KettleException e ) {
      assertEquals( BaseMessages.getString( PentahoMapReduceJobBuilderImpl.PKG,
        PentahoMapReduceJobBuilderImpl.PENTAHO_MAP_REDUCE_JOB_BUILDER_IMPL_NO_KEY_ORDINAL, inputStepName ),
        e.getMessage().trim() );
      throw e;
    }
  }

  @Test( expected = KettleException.class )
  public void testVerifyTransMetaNoValueOrdinal() throws KettleException {
    String inputStepName = "inputStepName";
    StepMeta inputStepMeta = mock( StepMeta.class );
    when( transMeta.findStep( inputStepName ) ).thenReturn( inputStepMeta );
    RowMetaInterface rowMetaInterface = mock( RowMetaInterface.class );
    when( rowMetaInterface.getFieldNames() ).thenReturn( new String[] { "key" } );
    when( transMeta.getStepFields( inputStepMeta ) ).thenReturn( rowMetaInterface );
    try {
      pentahoMapReduceJobBuilder.verifyTransMeta( transMeta, inputStepName, null );
    } catch ( KettleException e ) {
      assertEquals( BaseMessages.getString( PentahoMapReduceJobBuilderImpl.PKG,
        PentahoMapReduceJobBuilderImpl.PENTAHO_MAP_REDUCE_JOB_BUILDER_IMPL_NO_VALUE_ORDINAL, inputStepName ),
        e.getMessage().trim() );
      throw e;
    }
  }

  @Test( expected = KettleException.class )
  public void testVerifyTransMetaInputHopDisabled() throws KettleException {
    String inputStepName = "inputStepName";
    StepMeta inputStepMeta = mock( StepMeta.class );
    when( transMeta.findStep( inputStepName ) ).thenReturn( inputStepMeta );
    RowMetaInterface rowMetaInterface = mock( RowMetaInterface.class );
    when( rowMetaInterface.getFieldNames() ).thenReturn( new String[] { "key", "value" } );
    when( transMeta.getStepFields( inputStepMeta ) ).thenReturn( rowMetaInterface );
    when( transFactory.create( transMeta ) ).thenReturn( mock( Trans.class ) );
    try {
      pentahoMapReduceJobBuilder.verifyTransMeta( transMeta, inputStepName, null );
    } catch ( KettleException e ) {
      assertEquals( BaseMessages.getString( PentahoMapReduceJobBuilderImpl.PKG,
        PentahoMapReduceJobBuilderImpl.PENTAHO_MAP_REDUCE_JOB_BUILDER_IMPL_INPUT_HOP_DISABLED, inputStepName ),
        e.getMessage().trim() );
      throw e;
    }
  }

  @Test( expected = KettleException.class )
  public void testVerifyTransMetaOutputStepNotSpecified() throws KettleException {
    String inputStepName = "inputStepName";
    StepMeta inputStepMeta = mock( StepMeta.class );
    when( transMeta.findStep( inputStepName ) ).thenReturn( inputStepMeta );
    RowMetaInterface rowMetaInterface = mock( RowMetaInterface.class );
    when( rowMetaInterface.getFieldNames() ).thenReturn( new String[] { "key", "value" } );
    when( transMeta.getStepFields( inputStepMeta ) ).thenReturn( rowMetaInterface );
    Trans trans = mock( Trans.class );
    when( transFactory.create( transMeta ) ).thenReturn( trans );
    when( trans.getStepInterface( inputStepName, 0 ) ).thenReturn( mock( StepInterface.class ) );
    try {
      pentahoMapReduceJobBuilder.verifyTransMeta( transMeta, inputStepName, null );
    } catch ( KettleException e ) {
      assertEquals( BaseMessages.getString( PentahoMapReduceJobBuilderImpl.PKG,
        PentahoMapReduceJobBuilderImpl.PENTAHO_MAP_REDUCE_JOB_BUILDER_IMPL_OUTPUT_STEP_NOT_SPECIFIED ),
        e.getMessage().trim() );
      throw e;
    }
  }

  @Test( expected = KettleException.class )
  public void testVerifyTransMetaOutputStepNotFound() throws KettleException {
    String inputStepName = "inputStepName";
    String outputStepName = "outputStepName";
    StepMeta inputStepMeta = mock( StepMeta.class );
    when( transMeta.findStep( inputStepName ) ).thenReturn( inputStepMeta );
    RowMetaInterface rowMetaInterface = mock( RowMetaInterface.class );
    when( rowMetaInterface.getFieldNames() ).thenReturn( new String[] { "key", "value" } );
    when( transMeta.getStepFields( inputStepMeta ) ).thenReturn( rowMetaInterface );
    Trans trans = mock( Trans.class );
    when( transFactory.create( transMeta ) ).thenReturn( trans );
    when( trans.getStepInterface( inputStepName, 0 ) ).thenReturn( mock( StepInterface.class ) );
    try {
      pentahoMapReduceJobBuilder.verifyTransMeta( transMeta, inputStepName, outputStepName );
    } catch ( KettleException e ) {
      assertEquals( BaseMessages.getString( PentahoMapReduceJobBuilderImpl.PKG,
        PentahoMapReduceJobBuilderImpl.PENTAHO_MAP_REDUCE_JOB_BUILDER_IMPL_OUTPUT_STEP_NOT_FOUND, outputStepName ),
        e.getMessage().trim() );
      throw e;
    }
  }

  @Test( expected = KettleException.class )
  public void testVerifyTransMetaCheckException() throws KettleException {
    String inputStepName = "inputStepName";
    String outputStepName = "outputStepName";
    StepMeta inputStepMeta = mock( StepMeta.class );
    when( transMeta.findStep( inputStepName ) ).thenReturn( inputStepMeta );
    RowMetaInterface rowMetaInterface = mock( RowMetaInterface.class );
    when( rowMetaInterface.getFieldNames() ).thenReturn( new String[] { "key", "value" } );
    when( transMeta.getStepFields( inputStepMeta ) ).thenReturn( rowMetaInterface );
    Trans trans = mock( Trans.class );
    when( transFactory.create( transMeta ) ).thenReturn( trans );
    when( trans.getStepInterface( inputStepName, 0 ) ).thenReturn( mock( StepInterface.class ) );
    final StepMeta outputStepMeta = mock( StepMeta.class );
    PentahoMapReduceOutputStepMetaInterface pentahoMapReduceOutputStepMetaInterface =
      mock( PentahoMapReduceOutputStepMetaInterface.class );
    doAnswer( new Answer<Void>() {
      @Override public Void answer( InvocationOnMock invocation ) throws Throwable {
        List<CheckResultInterface> checkResultInterfaces = (List<CheckResultInterface>) invocation.getArguments()[ 0 ];
        checkResultInterfaces.add( new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, "test", outputStepMeta ) );
        return null;
      }
    } ).when( pentahoMapReduceOutputStepMetaInterface )
      .checkPmr( anyList(), eq( transMeta ), eq( outputStepMeta ), (RowMetaInterface) isNull() );
    when( outputStepMeta.getStepMetaInterface() ).thenReturn( pentahoMapReduceOutputStepMetaInterface );
    when( transMeta.findStep( outputStepName ) ).thenReturn( outputStepMeta );
    try {
      pentahoMapReduceJobBuilder.verifyTransMeta( transMeta, inputStepName, outputStepName );
    } catch ( KettleException e ) {
      assertTrue( e.getMessage().trim().startsWith( BaseMessages.getString( PentahoMapReduceJobBuilderImpl.PKG,
        PentahoMapReduceJobBuilderImpl.PENTAHO_MAP_REDUCE_JOB_BUILDER_IMPL_VALIDATION_ERROR ).trim() ) );
      throw e;
    }
  }

  @Test
  public void testVerifyTransMetaCheckSuccess() throws KettleException {
    String inputStepName = "inputStepName";
    String outputStepName = "outputStepName";
    StepMeta inputStepMeta = mock( StepMeta.class );
    when( transMeta.findStep( inputStepName ) ).thenReturn( inputStepMeta );
    RowMetaInterface rowMetaInterface = mock( RowMetaInterface.class );
    when( rowMetaInterface.getFieldNames() ).thenReturn( new String[] { "key", "value" } );
    when( transMeta.getStepFields( inputStepMeta ) ).thenReturn( rowMetaInterface );
    Trans trans = mock( Trans.class );
    when( transFactory.create( transMeta ) ).thenReturn( trans );
    when( trans.getStepInterface( inputStepName, 0 ) ).thenReturn( mock( StepInterface.class ) );
    final StepMeta outputStepMeta = mock( StepMeta.class );
    PentahoMapReduceOutputStepMetaInterface pentahoMapReduceOutputStepMetaInterface =
      mock( PentahoMapReduceOutputStepMetaInterface.class );
    when( outputStepMeta.getStepMetaInterface() ).thenReturn( pentahoMapReduceOutputStepMetaInterface );
    when( transMeta.findStep( outputStepName ) ).thenReturn( outputStepMeta );
    pentahoMapReduceJobBuilder.verifyTransMeta( transMeta, inputStepName, outputStepName );
  }

  @Test( expected = KettleException.class )
  public void testVerifyTransMetaOutKeyNotDefined() throws KettleException {
    String inputStepName = "inputStepName";
    String outputStepName = "outputStepName";
    StepMeta inputStepMeta = mock( StepMeta.class );
    when( transMeta.findStep( inputStepName ) ).thenReturn( inputStepMeta );
    RowMetaInterface rowMetaInterface = mock( RowMetaInterface.class );
    when( rowMetaInterface.getFieldNames() ).thenReturn( new String[] { "key", "value" } );
    when( transMeta.getStepFields( inputStepMeta ) ).thenReturn( rowMetaInterface );
    Trans trans = mock( Trans.class );
    when( transFactory.create( transMeta ) ).thenReturn( trans );
    when( trans.getStepInterface( inputStepName, 0 ) ).thenReturn( mock( StepInterface.class ) );
    final StepMeta outputStepMeta = mock( StepMeta.class );
    StepMetaInterface stepMetaInterface = mock( StepMetaInterface.class );
    when( outputStepMeta.getStepMetaInterface() ).thenReturn( stepMetaInterface );
    when( transMeta.findStep( outputStepName ) ).thenReturn( outputStepMeta );
    RowMetaInterface outputRowMetaInterface = mock( RowMetaInterface.class );
    when( transMeta.getStepFields( outputStepMeta ) ).thenReturn( outputRowMetaInterface );
    when( outputRowMetaInterface.getFieldNames() ).thenReturn( new String[] {} );
    try {
      pentahoMapReduceJobBuilder.verifyTransMeta( transMeta, inputStepName, outputStepName );
    } catch ( KettleException e ) {
      assertEquals( BaseMessages.getString( PentahoMapReduceJobBuilderImpl.PKG,
        PentahoMapReduceJobBuilderImpl.PENTAHO_MAP_REDUCE_JOB_BUILDER_IMPL_NO_OUTPUT_KEY_ORDINAL, outputStepName ),
        e.getMessage().trim() );
      throw e;
    }
  }

  @Test( expected = KettleException.class )
  public void testVerifyTransMetaOutValueNotDefined() throws KettleException {
    String inputStepName = "inputStepName";
    String outputStepName = "outputStepName";
    StepMeta inputStepMeta = mock( StepMeta.class );
    when( transMeta.findStep( inputStepName ) ).thenReturn( inputStepMeta );
    RowMetaInterface rowMetaInterface = mock( RowMetaInterface.class );
    when( rowMetaInterface.getFieldNames() ).thenReturn( new String[] { "key", "value" } );
    when( transMeta.getStepFields( inputStepMeta ) ).thenReturn( rowMetaInterface );
    Trans trans = mock( Trans.class );
    when( transFactory.create( transMeta ) ).thenReturn( trans );
    when( trans.getStepInterface( inputStepName, 0 ) ).thenReturn( mock( StepInterface.class ) );
    final StepMeta outputStepMeta = mock( StepMeta.class );
    StepMetaInterface stepMetaInterface = mock( StepMetaInterface.class );
    when( outputStepMeta.getStepMetaInterface() ).thenReturn( stepMetaInterface );
    when( transMeta.findStep( outputStepName ) ).thenReturn( outputStepMeta );
    RowMetaInterface outputRowMetaInterface = mock( RowMetaInterface.class );
    when( transMeta.getStepFields( outputStepMeta ) ).thenReturn( outputRowMetaInterface );
    when( outputRowMetaInterface.getFieldNames() ).thenReturn( new String[] { "outKey" } );
    try {
      pentahoMapReduceJobBuilder.verifyTransMeta( transMeta, inputStepName, outputStepName );
    } catch ( KettleException e ) {
      assertEquals( BaseMessages.getString( PentahoMapReduceJobBuilderImpl.PKG,
        PentahoMapReduceJobBuilderImpl.PENTAHO_MAP_REDUCE_JOB_BUILDER_IMPL_NO_OUTPUT_VALUE_ORDINAL, outputStepName ),
        e.getMessage().trim() );
      throw e;
    }
  }

  @Test
  public void testVerifyTransMetaOutSuccess() throws KettleException {
    String inputStepName = "inputStepName";
    String outputStepName = "outputStepName";
    StepMeta inputStepMeta = mock( StepMeta.class );
    when( transMeta.findStep( inputStepName ) ).thenReturn( inputStepMeta );
    RowMetaInterface rowMetaInterface = mock( RowMetaInterface.class );
    when( rowMetaInterface.getFieldNames() ).thenReturn( new String[] { "key", "value" } );
    when( transMeta.getStepFields( inputStepMeta ) ).thenReturn( rowMetaInterface );
    Trans trans = mock( Trans.class );
    when( transFactory.create( transMeta ) ).thenReturn( trans );
    when( trans.getStepInterface( inputStepName, 0 ) ).thenReturn( mock( StepInterface.class ) );
    final StepMeta outputStepMeta = mock( StepMeta.class );
    StepMetaInterface stepMetaInterface = mock( StepMetaInterface.class );
    when( outputStepMeta.getStepMetaInterface() ).thenReturn( stepMetaInterface );
    when( transMeta.findStep( outputStepName ) ).thenReturn( outputStepMeta );
    RowMetaInterface outputRowMetaInterface = mock( RowMetaInterface.class );
    when( transMeta.getStepFields( outputStepMeta ) ).thenReturn( outputRowMetaInterface );
    when( outputRowMetaInterface.getFieldNames() ).thenReturn( new String[] { "outKey", "outValue" } );
    pentahoMapReduceJobBuilder.verifyTransMeta( transMeta, inputStepName, outputStepName );
  }

  @Test
  public void testTransFactoryImpl() {
    TransMeta transMeta = mock( TransMeta.class );
    when( transMeta.listVariables() ).thenReturn( new String[ 0 ] );
    when( transMeta.listParameters() ).thenReturn( new String[ 0 ] );
    assertNotNull( new PentahoMapReduceJobBuilderImpl.TransFactory().create( transMeta ) );
  }

  @Test
  public void testCleanOutputPathFalse() throws IOException {
    Configuration configuration = mock( Configuration.class );
    pentahoMapReduceJobBuilder.cleanOutputPath( configuration );
    verifyNoMoreInteractions( configuration );
  }

  @Test
  public void testCleanOutputPathDoesntExist() throws IOException, URISyntaxException {
    pentahoMapReduceJobBuilder.setCleanOutputPath( true );
    String outputPath = "/test/path";
    pentahoMapReduceJobBuilder.setOutputPath( outputPath );
    Configuration configuration = mock( Configuration.class );
    String defaultFilesystemURL = "test:prefix";
    when( configuration.getDefaultFileSystemURL() ).thenReturn( defaultFilesystemURL );
    FileSystem fileSystem = mock( FileSystem.class );
    when( hadoopShim.getFileSystem( configuration ) ).thenReturn( fileSystem );
    Path path = mock( Path.class );
    when( path.toUri() ).thenReturn( new URI( "hdfs://test/uri" ) );
    when( fileSystem.asPath( defaultFilesystemURL, outputPath ) ).thenReturn( path );
    when( fileSystem.exists( path ) ).thenReturn( false );
    pentahoMapReduceJobBuilder.cleanOutputPath( configuration );
    verify( fileSystem, never() ).delete( any( Path.class ), anyBoolean() );
  }

  @Test
  public void testCleanOutputPathFailure() throws IOException, URISyntaxException {
    pentahoMapReduceJobBuilder.setCleanOutputPath( true );
    String outputPath = "/test/path";
    pentahoMapReduceJobBuilder.setOutputPath( outputPath );
    Configuration configuration = mock( Configuration.class );
    String defaultFilesystemURL = "test:prefix";
    when( configuration.getDefaultFileSystemURL() ).thenReturn( defaultFilesystemURL );
    FileSystem fileSystem = mock( FileSystem.class );
    when( hadoopShim.getFileSystem( configuration ) ).thenReturn( fileSystem );
    Path path = mock( Path.class );
    URI uri = new URI( "hdfs://test/uri" );
    when( path.toUri() ).thenReturn( uri );
    when( fileSystem.asPath( defaultFilesystemURL, outputPath ) ).thenReturn( path );
    when( fileSystem.exists( path ) ).thenReturn( true );
    when( fileSystem.delete( path, true ) ).thenReturn( false );
    when( logChannelInterface.isBasic() ).thenReturn( true );
    pentahoMapReduceJobBuilder.cleanOutputPath( configuration );
    verify( fileSystem ).delete( path, true );
    verify( logChannelInterface )
      .logBasic( BaseMessages
        .getString( PentahoMapReduceJobBuilderImpl.PKG,
          PentahoMapReduceJobBuilderImpl.JOB_ENTRY_HADOOP_TRANS_JOB_EXECUTOR_CLEANING_OUTPUT_PATH,
          uri.toString() ) );
    verify( logChannelInterface )
      .logBasic( BaseMessages
        .getString( PentahoMapReduceJobBuilderImpl.PKG,
          PentahoMapReduceJobBuilderImpl.JOB_ENTRY_HADOOP_TRANS_JOB_EXECUTOR_FAILED_TO_CLEAN_OUTPUT_PATH,
          uri.toString() ) );
  }

  @Test
  public void testCleanOutputPathFailureNoLog() throws IOException, URISyntaxException {
    pentahoMapReduceJobBuilder.setCleanOutputPath( true );
    String outputPath = "/test/path";
    pentahoMapReduceJobBuilder.setOutputPath( outputPath );
    Configuration configuration = mock( Configuration.class );
    String defaultFilesystemURL = "test:prefix";
    when( configuration.getDefaultFileSystemURL() ).thenReturn( defaultFilesystemURL );
    FileSystem fileSystem = mock( FileSystem.class );
    when( hadoopShim.getFileSystem( configuration ) ).thenReturn( fileSystem );
    Path path = mock( Path.class );
    URI uri = new URI( "hdfs://test/uri" );
    when( path.toUri() ).thenReturn( uri );
    when( fileSystem.asPath( defaultFilesystemURL, outputPath ) ).thenReturn( path );
    when( fileSystem.exists( path ) ).thenReturn( true );
    when( fileSystem.delete( path, true ) ).thenReturn( false );
    when( logChannelInterface.isBasic() ).thenReturn( false );
    pentahoMapReduceJobBuilder.cleanOutputPath( configuration );
    verify( fileSystem ).delete( path, true );
    verify( logChannelInterface, never() ).logBasic( anyString() );
  }

  @Test( expected = IOException.class )
  public void testCleanOutputPathException() throws IOException, URISyntaxException {
    pentahoMapReduceJobBuilder.setCleanOutputPath( true );
    String outputPath = "/test/path";
    pentahoMapReduceJobBuilder.setOutputPath( outputPath );
    Configuration configuration = mock( Configuration.class );
    String defaultFilesystemURL = "test:prefix";
    when( configuration.getDefaultFileSystemURL() ).thenReturn( defaultFilesystemURL );
    FileSystem fileSystem = mock( FileSystem.class );
    when( hadoopShim.getFileSystem( configuration ) ).thenReturn( fileSystem );
    Path path = mock( Path.class );
    URI uri = new URI( "hdfs://test/uri" );
    when( path.toUri() ).thenReturn( uri );
    when( fileSystem.asPath( defaultFilesystemURL, outputPath ) ).thenReturn( path );
    when( fileSystem.exists( path ) ).thenReturn( true );
    when( fileSystem.delete( path, true ) ).thenThrow( new IOException() );
    when( logChannelInterface.isBasic() ).thenReturn( false );
    pentahoMapReduceJobBuilder.cleanOutputPath( configuration );
  }

  @Test
  public void testCleanOutputPathSuccess() throws IOException, URISyntaxException {
    pentahoMapReduceJobBuilder.setCleanOutputPath( true );
    String outputPath = "/test/path";
    pentahoMapReduceJobBuilder.setOutputPath( outputPath );
    Configuration configuration = mock( Configuration.class );
    String defaultFilesystemURL = "test:prefix";
    when( configuration.getDefaultFileSystemURL() ).thenReturn( defaultFilesystemURL );
    FileSystem fileSystem = mock( FileSystem.class );
    when( hadoopShim.getFileSystem( configuration ) ).thenReturn( fileSystem );
    Path path = mock( Path.class );
    URI uri = new URI( "hdfs://test/uri" );
    when( path.toUri() ).thenReturn( uri );
    when( fileSystem.asPath( defaultFilesystemURL, outputPath ) ).thenReturn( path );
    when( fileSystem.exists( path ) ).thenReturn( true );
    when( fileSystem.delete( path, true ) ).thenReturn( true );
    when( logChannelInterface.isBasic() ).thenReturn( true );
    pentahoMapReduceJobBuilder.cleanOutputPath( configuration );
    verify( fileSystem ).delete( path, true );
    verify( logChannelInterface )
      .logBasic( BaseMessages
        .getString( PentahoMapReduceJobBuilderImpl.PKG,
          PentahoMapReduceJobBuilderImpl.JOB_ENTRY_HADOOP_TRANS_JOB_EXECUTOR_CLEANING_OUTPUT_PATH,
          uri.toString() ) );
    verify( logChannelInterface, times( 1 ) ).logBasic( anyString() );
  }

  @Test
  public void testGetPropertyFromConf() {
    Configuration configuration = mock( Configuration.class );
    Properties properties = mock( Properties.class );
    String property = "property";
    String value = "value";
    when( configuration.get( property ) ).thenReturn( value );
    assertEquals( value, pentahoMapReduceJobBuilder.getProperty( configuration, properties, property, null ) );
  }

  @Test
  public void testGetPropertyFromProperties() {
    Configuration configuration = mock( Configuration.class );
    Properties properties = mock( Properties.class );

    ArrayList<String> values = new ArrayList<>();
    values.add( "value1" );
    values.add( "value2" );
    when( properties.get( "property1" ) ).thenReturn( values );
    assertEquals( "value1,value2",
      pentahoMapReduceJobBuilder.getProperty( configuration, properties, "property1", null ) );

    when( properties.get( "property2" ) ).thenReturn( "value" );
    assertEquals( "value", pentahoMapReduceJobBuilder.getProperty( configuration, properties, "property2", null ) );
  }

  @Test
  public void testConfigureMinimal() throws Exception {
    pentahoMapReduceJobBuilder =
      spy( new PentahoMapReduceJobBuilderImpl( namedCluster, hadoopShim, logChannelInterface, variableSpace,
        pluginInterface, vfsPluginDirectory, pmrProperties, transFactory, pmrArchiveGetter, visitorServices ) );
    when( hadoopShim.getPentahoMapReduceMapRunnerClass() ).thenReturn( "" );
    pentahoMapReduceJobBuilder.setLogLevel( LogLevel.BASIC );
    pentahoMapReduceJobBuilder.setInputPaths( new String[ 0 ] );
    pentahoMapReduceJobBuilder.setOutputPath( "test" );
    pentahoMapReduceJobBuilder.setResolvedJarUrl( new URL( "file:///" ) );
    Configuration configuration = mock( Configuration.class );
    doAnswer( new Answer() {
      @Override public Object answer( InvocationOnMock invocation ) throws Throwable {
        return null;
      }
    } ).when( pentahoMapReduceJobBuilder ).configureVariableSpace( configuration );
    when( hadoopShim.getFileSystem( configuration ) ).thenReturn( mock( FileSystem.class ) );
    String testMrInput = "testMrInput";
    String testMrOutput = "testMrOutput";
    pentahoMapReduceJobBuilder.setMapperInfo( transXml, testMrInput, testMrOutput );
    pentahoMapReduceJobBuilder.configure( configuration );

    verify( configuration ).setMapRunnerClass( "" );
    verify( configuration ).set( PentahoMapReduceJobBuilderImpl.TRANSFORMATION_MAP_XML, transXml );
    verify( configuration ).set( PentahoMapReduceJobBuilderImpl.TRANSFORMATION_MAP_INPUT_STEPNAME, testMrInput );
    verify( configuration ).set( PentahoMapReduceJobBuilderImpl.TRANSFORMATION_MAP_OUTPUT_STEPNAME, testMrOutput );
    verify( configuration ).setJarByClass( Class.forName( "org.pentaho.hadoop.mapreduce.PentahoMapReduceJarMarker" ) );
    verify( configuration ).set( PentahoMapReduceJobBuilderImpl.LOG_LEVEL, LogLevel.BASIC.toString() );

    verify( pentahoMapReduceJobBuilder ).configureVariableSpace( configuration );

    verify( configuration, never() ).setCombinerClass( any( Class.class ) );
    verify( configuration, never() ).setReducerClass( any( Class.class ) );
  }

  @Test
  public void testConfigureFull() throws Exception {
    when( hadoopShim.getPentahoMapReduceMapRunnerClass() ).thenReturn( "" );
    when( hadoopShim.getPentahoMapReduceCombinerClass() )
      .thenReturn( "org.pentaho.hadoop.mapreduce.GenericTransCombiner" );
    when( hadoopShim.getPentahoMapReduceReducerClass() )
      .thenReturn( "org.pentaho.hadoop.mapreduce.GenericTransReduce" );
    pentahoMapReduceJobBuilder.setLogLevel( LogLevel.BASIC );
    pentahoMapReduceJobBuilder.setInputPaths( new String[ 0 ] );
    pentahoMapReduceJobBuilder.setOutputPath( "test" );
    pentahoMapReduceJobBuilder.setResolvedJarUrl( new URL( "file:///" ) );
    Configuration configuration = mock( Configuration.class );
    when( hadoopShim.getFileSystem( configuration ) ).thenReturn( mock( FileSystem.class ) );
    String testMrInput = "testMrInput";
    String testMrOutput = "testMrOutput";
    pentahoMapReduceJobBuilder.setMapperInfo( transXml, testMrInput, testMrOutput );
    String combinerInputStep = "testCInput";
    String combinerOutputStep = "testCOutput";
    pentahoMapReduceJobBuilder.setCombinerInfo( transXml, combinerInputStep, combinerOutputStep );
    String testRInput = "testRInput";
    String testROutput = "testROutput";
    pentahoMapReduceJobBuilder.setReducerInfo( transXml, testRInput, testROutput );
    pentahoMapReduceJobBuilder.configure( configuration );

    verify( configuration ).setMapRunnerClass( "" );
    verify( configuration ).set( PentahoMapReduceJobBuilderImpl.TRANSFORMATION_MAP_XML, transXml );
    verify( configuration ).set( PentahoMapReduceJobBuilderImpl.TRANSFORMATION_MAP_INPUT_STEPNAME, testMrInput );
    verify( configuration ).set( PentahoMapReduceJobBuilderImpl.TRANSFORMATION_MAP_OUTPUT_STEPNAME, testMrOutput );

    verify( configuration )
      .set( PentahoMapReduceJobBuilderImpl.TRANSFORMATION_COMBINER_XML, transXml );
    verify( configuration )
      .set( PentahoMapReduceJobBuilderImpl.TRANSFORMATION_COMBINER_INPUT_STEPNAME, combinerInputStep );
    verify( configuration )
      .set( PentahoMapReduceJobBuilderImpl.TRANSFORMATION_COMBINER_OUTPUT_STEPNAME, combinerOutputStep );
    verify( configuration ).setCombinerClass( Class.forName( "org.pentaho.hadoop.mapreduce.GenericTransCombiner" ) );

    verify( configuration ).set( PentahoMapReduceJobBuilderImpl.TRANSFORMATION_REDUCE_XML, transXml );
    verify( configuration ).set( PentahoMapReduceJobBuilderImpl.TRANSFORMATION_REDUCE_INPUT_STEPNAME, testRInput );
    verify( configuration ).set( PentahoMapReduceJobBuilderImpl.TRANSFORMATION_REDUCE_OUTPUT_STEPNAME, testROutput );
    verify( configuration ).setReducerClass( Class.forName( "org.pentaho.hadoop.mapreduce.GenericTransReduce" ) );

    verify( configuration ).setJarByClass( Class.forName( "org.pentaho.hadoop.mapreduce.PentahoMapReduceJarMarker" ) );
    verify( configuration ).set( PentahoMapReduceJobBuilderImpl.LOG_LEVEL, LogLevel.BASIC.toString() );
  }

  @Test
  public void testDeleteLogging() throws Exception {
    when( hadoopShim.getPentahoMapReduceMapRunnerClass() ).thenReturn( "" );
    pentahoMapReduceJobBuilder.setLogLevel( LogLevel.BASIC );
    pentahoMapReduceJobBuilder.setInputPaths( new String[ 0 ] );
    pentahoMapReduceJobBuilder.setOutputPath( "test" );
    pentahoMapReduceJobBuilder.setResolvedJarUrl( new URL( "file:///" ) );
    Configuration configuration = mock( Configuration.class );
    when( hadoopShim.getFileSystem( configuration ) ).thenReturn( mock( FileSystem.class ) );
    String testMrInput = "testMrInput";
    String testMrOutput = "testMrOutput";
    TransMeta meta = new TransMeta();
    TransConfiguration transConfig = new TransConfiguration( meta, new TransExecutionConfiguration() );
    String transXmlWOLogging = TransConfiguration.fromXML( transConfig.getXML() ).getXML();
    meta = transConfig.getTransMeta();

    HasDatabasesInterface dbIf = mock( HasDatabasesInterface.class );
    VariableSpace vsLogging = variableSpace;

    MetricsLogTable metricsLogTable = MetricsLogTable.getDefault( vsLogging, dbIf );
    metricsLogTable.setConnectionName( "logging-connection" );
    meta.setMetricsLogTable( metricsLogTable );

    PerformanceLogTable performanceLogTable = PerformanceLogTable.getDefault( vsLogging, dbIf );
    performanceLogTable.setConnectionName( "logging-connection" );
    meta.setPerformanceLogTable( performanceLogTable );

    StepLogTable stepLogTable = StepLogTable.getDefault( vsLogging, dbIf );
    stepLogTable.setConnectionName( "logging-connection" );
    meta.setStepLogTable( stepLogTable );

    TransLogTable transLogTable = TransLogTable.getDefault( vsLogging, dbIf, null );
    transLogTable.setConnectionName( "logging-connection" );
    meta.setTransLogTable( transLogTable );

    ChannelLogTable channelLogTable = ChannelLogTable.getDefault( vsLogging, mock( HasDatabasesInterface.class ) );
    channelLogTable.setConnectionName( "logging-connection" );
    meta.setChannelLogTable( channelLogTable );

    transConfig.setTransMeta( meta );
    String logTransXml = TransConfiguration.fromXML( transConfig.getXML() ).getXML();

    pentahoMapReduceJobBuilder.setMapperInfo( logTransXml, testMrInput, testMrOutput );
    pentahoMapReduceJobBuilder.configure( configuration );

    verify( configuration ).setMapRunnerClass( "" );
    verify( configuration ).set( PentahoMapReduceJobBuilderImpl.TRANSFORMATION_MAP_XML, transXmlWOLogging );
    verify( configuration ).set( PentahoMapReduceJobBuilderImpl.TRANSFORMATION_MAP_INPUT_STEPNAME, testMrInput );
    verify( configuration ).set( PentahoMapReduceJobBuilderImpl.TRANSFORMATION_MAP_OUTPUT_STEPNAME, testMrOutput );
    verify( configuration ).setJarByClass( Class.forName( "org.pentaho.hadoop.mapreduce.PentahoMapReduceJarMarker" ) );
    verify( configuration ).set( PentahoMapReduceJobBuilderImpl.LOG_LEVEL, LogLevel.BASIC.toString() );
  }

  @Test
  public void testConvert1() throws Exception {
    expectedEx.expect( RuntimeException.class );
    expectedEx.expectMessage( "Unable to convert string to object" );

    Configuration configuration = mock( Configuration.class );
    pentahoMapReduceJobBuilder.setMapperInfo( "bad", "testMrInput", "testMrOutput" );
    pentahoMapReduceJobBuilder.configure( configuration );
  }

  @Test
  public void testConvert2() throws Exception {
    expectedEx.expect( RuntimeException.class );
    expectedEx.expectMessage( "Unable to convert object to string" );

    Configuration configuration = mock( Configuration.class );
    pentahoMapReduceJobBuilder.setMapperInfo( transXml, "testMrInput", "testMrOutput" );

    List<TransformationVisitorService> badServices = new ArrayList<>();

    badServices.add( ( transformations, namedCluster ) -> {
      try {
        TransConfiguration transConfig = mock( TransConfiguration.class );
        when( transConfig.getXML() ).thenThrow( new IOException( "Some error" ) );
        transformations.setMapper( Optional.of( transConfig ) );
      } catch ( Exception e ) {
        throw new RuntimeException( e );
      }
    } );

    PentahoMapReduceJobBuilderImpl builder =
      new PentahoMapReduceJobBuilderImpl( namedCluster, hadoopShim, logChannelInterface, variableSpace,
        pluginInterface, vfsPluginDirectory, pmrProperties, transFactory, pmrArchiveGetter, badServices );
    builder.configure( configuration );
  }

  @Test
  public void testSubmitNoDistributedCache() throws IOException {
    Configuration conf = mock( Configuration.class );
    JobConf jobConf = mock( JobConf.class );
    when( jobConf.getCredentials() ).thenReturn( new Credentials() );
    when( conf.getAsDelegateConf( any() ) ).thenReturn( jobConf );
    when( conf.get( PentahoMapReduceJobBuilderImpl.PENTAHO_MAPREDUCE_PROPERTY_USE_DISTRIBUTED_CACHE ) )
      .thenReturn( Boolean.toString( false ) );
    pentahoMapReduceJobBuilder.submit( conf, "" );
    verify( hadoopShim ).submitJob( conf );
  }

  @Test( expected = IOException.class )
  public void testSubmitNoInstallPath() throws IOException {
    Configuration conf = mock( Configuration.class );
    when( conf.get( PentahoMapReduceJobBuilderImpl.PENTAHO_MAPREDUCE_PROPERTY_USE_DISTRIBUTED_CACHE ) )
      .thenReturn( "true" );
    try {
      pentahoMapReduceJobBuilder.submit( conf, "" );
    } catch ( IOException e ) {
      assertEquals( BaseMessages.getString( PentahoMapReduceJobBuilderImpl.PKG,
        PentahoMapReduceJobBuilderImpl.JOB_ENTRY_HADOOP_TRANS_JOB_EXECUTOR_INSTALLATION_OF_KETTLE_FAILED ),
        e.getMessage() );
      assertEquals( BaseMessages.getString( PentahoMapReduceJobBuilderImpl.PKG,
        PentahoMapReduceJobBuilderImpl.JOB_ENTRY_HADOOP_TRANS_JOB_EXECUTOR_KETTLE_HDFS_INSTALL_DIR_MISSING ),
        e.getCause().getMessage() );
      throw e;
    }
  }

  @Test
  public void testSubmitEmptyInstallId() throws IOException {
    Configuration conf = mock( Configuration.class );
    FileSystem fileSystem = mock( FileSystem.class );
    when( hadoopShim.getFileSystem( conf ) ).thenReturn( fileSystem );
    when( conf.get( PentahoMapReduceJobBuilderImpl.PENTAHO_MAPREDUCE_PROPERTY_USE_DISTRIBUTED_CACHE ) )
      .thenReturn( "true" );
    String installPath = "/path" + Const.FILE_SEPARATOR;
    when( conf.get( PentahoMapReduceJobBuilderImpl.PENTAHO_MAPREDUCE_PROPERTY_KETTLE_HDFS_INSTALL_DIR ) )
      .thenReturn( installPath );
    try {
      pentahoMapReduceJobBuilder.submit( conf, "" );
    } catch ( IOException e ) {
      // Ignore
    }
    verify( fileSystem ).asPath( installPath, pentahoMapReduceJobBuilder.getInstallId() );
  }

  @Test
  public void testSubmitAlreadyInstalled() throws Exception {
    Configuration conf = mock( Configuration.class );
    JobConf jobConf = mock( JobConf.class );
    when( jobConf.getCredentials() ).thenReturn( new Credentials() );

    when( conf.getAsDelegateConf( any() ) ).thenReturn( jobConf );
    FileSystem fileSystem = mock( FileSystem.class );
    DistributedCacheUtil distributedCacheUtil = mock( DistributedCacheUtil.class );
    Path kettleEnvInstallDir = mock( Path.class );
    URI kettleEnvInstallDirUri = new URI( "http://testUri/path" );
    when( kettleEnvInstallDir.toUri() ).thenReturn( kettleEnvInstallDirUri );

    when( hadoopShim.getFileSystem( conf ) ).thenReturn( fileSystem );
    when( hadoopShim.getDistributedCacheUtil() ).thenReturn( distributedCacheUtil );
    when( conf.get( PentahoMapReduceJobBuilderImpl.PENTAHO_MAPREDUCE_PROPERTY_USE_DISTRIBUTED_CACHE ) )
      .thenReturn( "true" );
    String installPath = "/path" + Const.FILE_SEPARATOR;
    when( conf.get( PentahoMapReduceJobBuilderImpl.PENTAHO_MAPREDUCE_PROPERTY_KETTLE_HDFS_INSTALL_DIR ) )
      .thenReturn( installPath );
    String installId = "install_id";
    when( conf.get( PentahoMapReduceJobBuilderImpl.PENTAHO_MAPREDUCE_PROPERTY_KETTLE_INSTALLATION_ID ) )
      .thenReturn( installId );
    when( fileSystem.asPath( installPath, installId ) ).thenReturn( kettleEnvInstallDir );
    when( distributedCacheUtil.isKettleEnvironmentInstalledAt( fileSystem, kettleEnvInstallDir ) ).thenReturn( true );
    String mapreduceClasspath = "mapreduceClasspath";
    when( conf.get( PentahoMapReduceJobBuilderImpl.MAPREDUCE_APPLICATION_CLASSPATH,
      PentahoMapReduceJobBuilderImpl.DEFAULT_MAPREDUCE_APPLICATION_CLASSPATH ) ).thenReturn( mapreduceClasspath );

    pentahoMapReduceJobBuilder.submit( conf, "" );
    verify( logChannelInterface ).logBasic( BaseMessages.getString( PentahoMapReduceJobBuilderImpl.PKG,
      PentahoMapReduceJobBuilderImpl.JOB_ENTRY_HADOOP_TRANS_JOB_EXECUTOR_CONFIGURING_JOB_WITH_KETTLE_AT,
      kettleEnvInstallDirUri.getPath() ) );
    verify( conf ).set( PentahoMapReduceJobBuilderImpl.MAPREDUCE_APPLICATION_CLASSPATH,
      PentahoMapReduceJobBuilderImpl.CLASSES + mapreduceClasspath );
    verify( distributedCacheUtil ).configureWithKettleEnvironment( conf, fileSystem, kettleEnvInstallDir );
  }

  @Test( expected = IOException.class )
  public void testSubmitNoPmrArchive() throws IOException, ConfigurationException, URISyntaxException {
    Configuration conf = mock( Configuration.class );
    FileSystem fileSystem = mock( FileSystem.class );
    DistributedCacheUtil distributedCacheUtil = mock( DistributedCacheUtil.class );
    Path kettleEnvInstallDir = mock( Path.class );
    URI kettleEnvInstallDirUri = new URI( "http://testUri/path" );

    when( hadoopShim.getFileSystem( conf ) ).thenReturn( fileSystem );
    when( hadoopShim.getDistributedCacheUtil() ).thenReturn( distributedCacheUtil );
    when( conf.get( PentahoMapReduceJobBuilderImpl.PENTAHO_MAPREDUCE_PROPERTY_USE_DISTRIBUTED_CACHE ) )
      .thenReturn( "true" );
    String installPath = "/path" + Const.FILE_SEPARATOR;
    when( conf.get( PentahoMapReduceJobBuilderImpl.PENTAHO_MAPREDUCE_PROPERTY_KETTLE_HDFS_INSTALL_DIR ) )
      .thenReturn( installPath );
    String installId = "install_id";
    when( conf.get( PentahoMapReduceJobBuilderImpl.PENTAHO_MAPREDUCE_PROPERTY_KETTLE_INSTALLATION_ID ) )
      .thenReturn( installId );
    when( fileSystem.asPath( installPath, installId ) ).thenReturn( kettleEnvInstallDir );
    when( distributedCacheUtil.isKettleEnvironmentInstalledAt( fileSystem, kettleEnvInstallDir ) ).thenReturn( false );
    String archiveName = "archiveName";
    when( pmrArchiveGetter.getVfsFilename( conf ) ).thenReturn( archiveName );

    try {
      pentahoMapReduceJobBuilder.submit( conf, "" );
    } catch ( IOException e ) {
      assertEquals( BaseMessages.getString( PentahoMapReduceJobBuilderImpl.PKG,
        PentahoMapReduceJobBuilderImpl.JOB_ENTRY_HADOOP_TRANS_JOB_EXECUTOR_INSTALLATION_OF_KETTLE_FAILED ),
        e.getMessage() );
      assertEquals( BaseMessages.getString( PentahoMapReduceJobBuilderImpl.PKG,
        PentahoMapReduceJobBuilderImpl.JOB_ENTRY_HADOOP_TRANS_JOB_EXECUTOR_UNABLE_TO_LOCATE_ARCHIVE, archiveName )
        .trim(), e.getCause().getMessage().trim() );
      throw e;
    }
  }

  //todo: revise here
  @Test( /*expected = IOException.class*/ )
  public void testSubmitInstallFail()
    throws URISyntaxException, IOException, ConfigurationException, KettleFileException {
    Configuration conf = mock( Configuration.class );
    FileSystem fileSystem = mock( FileSystem.class );
    DistributedCacheUtil distributedCacheUtil = mock( DistributedCacheUtil.class );
    Path kettleEnvInstallDir = mock( Path.class );
    URI kettleEnvInstallDirUri = new URI( "http://testUri/path" );
    when( kettleEnvInstallDir.toUri() ).thenReturn( kettleEnvInstallDirUri );

    when( hadoopShim.getFileSystem( conf ) ).thenReturn( fileSystem );
    when( hadoopShim.getDistributedCacheUtil() ).thenReturn( distributedCacheUtil );
    when( conf.get( PentahoMapReduceJobBuilderImpl.PENTAHO_MAPREDUCE_PROPERTY_USE_DISTRIBUTED_CACHE ) )
      .thenReturn( "true" );
    String installPath = "/path";
    when( conf.get( PentahoMapReduceJobBuilderImpl.PENTAHO_MAPREDUCE_PROPERTY_KETTLE_HDFS_INSTALL_DIR ) )
      .thenReturn( installPath );
    JobConf jobConf = mock( JobConf.class );
    when( jobConf.getCredentials() ).thenReturn( new Credentials() );
    when( conf.getAsDelegateConf( any() ) ).thenReturn( jobConf );

    installPath += Const.FILE_SEPARATOR;
    String installId = "install_id";
    when( conf.get( PentahoMapReduceJobBuilderImpl.PENTAHO_MAPREDUCE_PROPERTY_KETTLE_INSTALLATION_ID ) )
      .thenReturn( installId );
    when( fileSystem.asPath( installPath, installId ) ).thenReturn( kettleEnvInstallDir );
    when( distributedCacheUtil.isKettleEnvironmentInstalledAt( fileSystem, kettleEnvInstallDir ) ).thenReturn( false );
    String mapreduceClasspath = "mapreduceClasspath";
    when( conf.get( PentahoMapReduceJobBuilderImpl.MAPREDUCE_APPLICATION_CLASSPATH,
      PentahoMapReduceJobBuilderImpl.DEFAULT_MAPREDUCE_APPLICATION_CLASSPATH ) ).thenReturn( mapreduceClasspath );
    when( pmrArchiveGetter.getPmrArchive( conf ) ).thenReturn( mock( FileObject.class ) );

    try {
      pentahoMapReduceJobBuilder.submit( conf, "" );
    } catch ( IOException e ) {
      assertEquals( BaseMessages.getString( PentahoMapReduceJobBuilderImpl.PKG,
        PentahoMapReduceJobBuilderImpl.JOB_ENTRY_HADOOP_TRANS_JOB_EXECUTOR_INSTALLATION_OF_KETTLE_FAILED ),
        e.getMessage() );
      assertEquals( BaseMessages.getString( PentahoMapReduceJobBuilderImpl.PKG,
        PentahoMapReduceJobBuilderImpl.JOB_ENTRY_HADOOP_TRANS_JOB_EXECUTOR_KETTLE_INSTALLATION_MISSING_FROM,
        kettleEnvInstallDirUri.getPath() ).trim(), e.getCause().getMessage().trim() );
      throw e;
    }
  }

  @Test
  public void testSubmitInstallSucceed()
    throws Exception {
    Configuration conf = mock( Configuration.class );
    JobConf jobConf = mock( JobConf.class );
    when( jobConf.getCredentials() ).thenReturn( new Credentials() );
    when( conf.getAsDelegateConf( any() ) ).thenReturn( jobConf );

    FileSystem fileSystem = mock( FileSystem.class );
    DistributedCacheUtil distributedCacheUtil = mock( DistributedCacheUtil.class );
    Path kettleEnvInstallDir = mock( Path.class );
    URI kettleEnvInstallDirUri = new URI( "http://testUri/path" );
    when( kettleEnvInstallDir.toUri() ).thenReturn( kettleEnvInstallDirUri );

    when( hadoopShim.getFileSystem( conf ) ).thenReturn( fileSystem );
    when( hadoopShim.getDistributedCacheUtil() ).thenReturn( distributedCacheUtil );
    when( conf.get( PentahoMapReduceJobBuilderImpl.PENTAHO_MAPREDUCE_PROPERTY_USE_DISTRIBUTED_CACHE ) )
      .thenReturn( "true" );
    String installPath = "/path" + Const.FILE_SEPARATOR;
    when( conf.get( PentahoMapReduceJobBuilderImpl.PENTAHO_MAPREDUCE_PROPERTY_KETTLE_HDFS_INSTALL_DIR ) )
      .thenReturn( installPath );
    String installId = "install_id";
    when( conf.get( PentahoMapReduceJobBuilderImpl.PENTAHO_MAPREDUCE_PROPERTY_KETTLE_INSTALLATION_ID ) )
      .thenReturn( installId );
    when( fileSystem.asPath( installPath, installId ) ).thenReturn( kettleEnvInstallDir );
    when( distributedCacheUtil.isKettleEnvironmentInstalledAt( fileSystem, kettleEnvInstallDir ) ).thenReturn( false )
      .thenReturn( true );
    String mapreduceClasspath = "mapreduceClasspath";
    when( conf.get( PentahoMapReduceJobBuilderImpl.MAPREDUCE_APPLICATION_CLASSPATH,
      PentahoMapReduceJobBuilderImpl.DEFAULT_MAPREDUCE_APPLICATION_CLASSPATH ) ).thenReturn( mapreduceClasspath );
    when( pmrArchiveGetter.getPmrArchive( conf ) ).thenReturn( mock( FileObject.class ) );

    pentahoMapReduceJobBuilder.submit( conf, "" );

    verify( logChannelInterface ).logBasic( BaseMessages.getString( PentahoMapReduceJobBuilderImpl.PKG,
      PentahoMapReduceJobBuilderImpl.JOB_ENTRY_HADOOP_TRANS_JOB_EXECUTOR_CONFIGURING_JOB_WITH_KETTLE_AT,
      kettleEnvInstallDirUri.getPath() ) );
    verify( conf ).set( PentahoMapReduceJobBuilderImpl.MAPREDUCE_APPLICATION_CLASSPATH,
      PentahoMapReduceJobBuilderImpl.CLASSES + mapreduceClasspath );
    verify( distributedCacheUtil ).configureWithKettleEnvironment( conf, fileSystem, kettleEnvInstallDir );
  }
}
