/**
 * ****************************************************************************
 * <p/>
 * Pentaho Big Data
 * <p/>
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
 * <p/>
 * ******************************************************************************
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 * <p/>
 * ****************************************************************************
 */

package org.pentaho.hbase.shim.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.pentaho.di.core.logging.KettleLogStore;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.logging.LogChannelInterfaceFactory;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.hbase.factory.HBaseAdmin;
import org.pentaho.hbase.factory.HBaseClientFactory;
import org.pentaho.hbase.factory.HBasePut;
import org.pentaho.hbase.factory.HBaseTable;
import org.pentaho.hbase.shim.api.ColumnFilter;
import org.pentaho.hbase.shim.api.HBaseValueMeta;
import org.pentaho.hbase.shim.spi.HBaseBytesUtilShim;
import org.pentaho.hbase.shim.spi.HBaseConnection;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NavigableMap;
import java.util.Properties;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class CommonHBaseConnectionTest {
  public static final String ZOOKEEPER_QUORUM_CONFIG_TEST_FILE = "otherHost1:7222,otherHost2:7222,otherHost3:7222";
  private CommonHBaseConnection commonHBaseConnection;
  private CommonHBaseConnection connectionSpy;
  private static LogChannelInterfaceFactory oldLogChannelInterfaceFactory;
  private static LogChannelInterface logChannelInterface;
  private Class PKG = CommonHBaseConnection.class;

  @Rule
  public ExpectedException thrown = ExpectedException.none();
  private Properties properties;
  private HBaseAdmin hbaseAdminMock;
  private static File confFile;

  @BeforeClass
  public static void beforeClass() throws Exception {
    oldLogChannelInterfaceFactory = KettleLogStore.getLogChannelInterfaceFactory();
    setKettleLogFactoryWithMock();
    addToCustomHbaseConfigFile();
  }

  @Before
  public void setUp() throws Exception {
    commonHBaseConnection =
      (CommonHBaseConnection) Class.forName("org.pentaho.hbase.shim.common.HBaseConnectionImpl").newInstance();;
    hbaseAdminMock = mock( HBaseAdmin.class );
    commonHBaseConnection.m_admin = hbaseAdminMock;
    connectionSpy = spy( commonHBaseConnection );
    properties = new Properties();
  }

  @Test
  public void testConfigureConnection() throws Exception {
    HBaseClientFactory mock = mock( HBaseClientFactory.class );
    HBaseAdmin hbaseAdmin = mock( HBaseAdmin.class );
    when( mock.getHBaseAdmin() ).thenReturn( hbaseAdmin );
    doReturn( mock ).when( connectionSpy ).getHBaseClientFactory( any( Configuration.class ) );

    connectionSpy.configureConnection( new Properties(), null );

    assertNotNull( connectionSpy.m_config );
    assertNotNull( connectionSpy.m_factory );
    assertEquals( hbaseAdmin, connectionSpy.m_admin );
  }

  private static void addToCustomHbaseConfigFile() throws Exception {
    Configuration configuration = new Configuration();
    configuration.addResource( "hbase-default.xml" );
    if ( configuration.get( HBaseConnection.HBASE_VERSION_KEY ) != null ) {
      Configuration conf = new Configuration( false );
      //if we don't set hbase version according to valid one in shim exception is thrown, so
      //in new file we set the versio property read from default one from hadoop libraries for provider
      conf.set( HBaseConnection.HBASE_VERSION_KEY, configuration.get( HBaseConnection.HBASE_VERSION_KEY ) );
      conf.set( HBaseConnection.ZOOKEEPER_QUORUM_KEY, ZOOKEEPER_QUORUM_CONFIG_TEST_FILE );
      confFile = new File( System.getProperty( "user.dir" ) + File.separator + "hbase-default-1.xml" );
      FileOutputStream out = new FileOutputStream( confFile );
      conf.writeXml( new DataOutputStream( out ) );
      out.close();
    }
  }

  private void useOverriddenConfigurationFile( Properties properties ) {
    useMockForHBaseClientFactory();
    if ( confFile != null && confFile.exists() ) {
      //set custom configuration file place, as in default from provider library set zookeeper to localhost
      properties.setProperty( HBaseConnection.DEFAULTS_KEY, confFile.toURI().getPath() );
    }
  }

  @Test
  public void testErrorMismatchClientZookeeperQuorumWithConfigurationQuorum() throws Exception {
    String clientZookeeperQuorum = "testHost1:7222,testHost2:7222,testHost3:7222";
    properties.put( "hbase.zookeeper.quorum", clientZookeeperQuorum );
    useOverriddenConfigurationFile( properties );
    connectionSpy.configureConnection( properties, null );
    String errorMessageMismatch = BaseMessages.
      getString( PKG, "CommonHBaseConnection.Error.MismatchZookeeperNamedClusterVsConfiguration", clientZookeeperQuorum,
        ZOOKEEPER_QUORUM_CONFIG_TEST_FILE );
    verify( logChannelInterface, atLeast( 1 ) ).logBasic( errorMessageMismatch );
  }

  @Test
  public void testClientZookeeperQuorumWithoutPortsWithConfigurationQuorum() throws Exception {
    String clientZookeeperQuorum =
      "otherHost1.fullyQualified.com,otherHost2.fullyQualified.com,otherHost3.fullyQualified.com";
    properties.put( "hbase.zookeeper.quorum", clientZookeeperQuorum );
    useOverriddenConfigurationFile( properties );
    connectionSpy.configureConnection( properties, null );
    assertEquals( "otherHost1.fullyQualified.com,otherHost2.fullyQualified.com,otherHost3.fullyQualified.com",
      connectionSpy.m_config.get( "hbase.zookeeper.quorum" ) );
  }

  @Test
  public void testZookeeperValidQuorumOneServer() throws Exception {
    properties.put( "hbase.zookeeper.quorum", "otherHost1:7222" );
    useOverriddenConfigurationFile( properties );
    connectionSpy.configureConnection( properties, null );
    assertEquals( "otherHost1:7222", connectionSpy.m_config.get( "hbase.zookeeper.quorum" ) );
  }

  @Test
  public void testZookeeperValidQuorumSeveralServers() throws Exception {
    properties.put( "hbase.zookeeper.quorum", "otherHost1:7222,otherHost2:7222" );
    useOverriddenConfigurationFile( properties );
    connectionSpy.configureConnection( properties, null );
    assertEquals( "otherHost1:7222,otherHost2:7222", connectionSpy.m_config.get( "hbase.zookeeper.quorum" ) );
  }

  @Test
  public void testEmptyZookeeperServers() throws Exception {
    useMockForHBaseClientFactory();
    properties.put( "hbase.zookeeper.quorum", "" );
    useOverriddenConfigurationFile( properties );
    connectionSpy.configureConnection( properties, null );
    assertEquals( ZOOKEEPER_QUORUM_CONFIG_TEST_FILE, connectionSpy.m_config.get( "hbase.zookeeper.quorum" ) );
  }

  /**
   * set mock for log channel factory for skipping npe in tests
   */
  public static void setKettleLogFactoryWithMock() {
    LogChannelInterfaceFactory logChannelInterfaceFactory = mock( LogChannelInterfaceFactory.class );
    logChannelInterface = mock( LogChannelInterface.class );
    when( logChannelInterfaceFactory.create( any() ) ).thenReturn( logChannelInterface );
    KettleLogStore.setLogChannelInterfaceFactory( logChannelInterfaceFactory );
  }

  @Test
  public void testConfigureConnectionIncorrectHbaseDefaultUrl() throws Exception {
    thrown.expect( IllegalArgumentException.class );
    thrown.expectMessage( "Malformed configuration URL for HBase site/default" );

    properties.put( "hbase.default", ":invalid_url:" );
    commonHBaseConnection.configureConnection( properties, null );
  }

  @Test
  public void testConfigureConnectionIncorrectHbaseSiteUrl() throws Exception {
    thrown.expect( IllegalArgumentException.class );
    thrown.expectMessage( "Malformed configuration URL for HBase site/default" );

    properties.put( "hbase.site", ":invalid_url:" );
    commonHBaseConnection.configureConnection( properties, null );
  }

  @Test
  public void testConfigureConnectionSetZookeeperQuorum() throws Exception {
    useMockForHBaseClientFactory();
    properties.put( "hbase.zookeeper.quorum", "otherHost2:7222" );
    useOverriddenConfigurationFile( properties );

    connectionSpy.configureConnection( properties, null );
    assertEquals( "otherHost2:7222", connectionSpy.m_config.get( "hbase.zookeeper.quorum" ) );
  }

  @Test
  public void testConfigureConnectionSetZookeeperPort() throws Exception {
    useMockForHBaseClientFactory();
    properties.put( "hbase.zookeeper.property.clientPort", "5151" );

    connectionSpy.configureConnection( properties, null );
    assertEquals( 5151, connectionSpy.m_config.getInt( "hbase.zookeeper.property.clientPort", 2181 ) );
  }

  @Test
  public void testConfigureConnectionUnableToParseZookeeperPortMessage() throws Exception {
    useMockForHBaseClientFactory();
    ArrayList<String> messages = new ArrayList<>();
    properties.put( "hbase.zookeeper.property.clientPort", "astring" );

    connectionSpy.configureConnection( properties, messages );
    assertEquals( 1, messages.size() );
    assertEquals( "Unable to parse zookeeper port - using default", messages.get( 0 ) );
  }

  @Test
  public void testGetBytesUtil() throws Exception {
    HBaseBytesUtilShim bytesUtil = commonHBaseConnection.getBytesUtil();
    assertNotNull( bytesUtil );
  }

  @Test
  public void testCheckConfiguration() throws Exception {
    commonHBaseConnection.m_admin = null;
    thrown.expect( Exception.class );
    thrown.expectMessage( "Connection has not been configured yet" );
    commonHBaseConnection.checkConfiguration();
  }

  @Test
  public void testTableExists() throws Exception {
    when( hbaseAdminMock.tableExists( "existingTable" ) ).thenReturn( true );
    assertTrue( commonHBaseConnection.tableExists( "existingTable" ) );
  }

  @Test
  public void testTableNotExists() throws Exception {
    when( hbaseAdminMock.tableExists( "nonExistingTable" ) ).thenReturn( false );
    assertFalse( commonHBaseConnection.tableExists( "nonExistingTable" ) );
  }

  @Test
  public void testDisableTable() throws Exception {
    commonHBaseConnection.disableTable( "table1" );
    verify( hbaseAdminMock ).disableTable( "table1" );
  }

  @Test
  public void testEnableTable() throws Exception {
    commonHBaseConnection.enableTable( "table1" );
    verify( hbaseAdminMock ).enableTable( "table1" );
  }

  @Test
  public void testIsTableDisabled() throws Exception {
    when( hbaseAdminMock.isTableDisabled( "disabledTable" ) ).thenReturn( true );
    assertTrue( commonHBaseConnection.isTableDisabled( "disabledTable" ) );
  }

  @Test
  public void testIsTableEnabled() throws Exception {
    when( hbaseAdminMock.isTableDisabled( "enabledTable" ) ).thenReturn( false );
    assertFalse( commonHBaseConnection.isTableDisabled( "enabledTable" ) );
  }

  @Test
  public void testIsTableAvailable() throws Exception {
    when( hbaseAdminMock.isTableAvailable( "availableTable" ) ).thenReturn( true );
    assertTrue( commonHBaseConnection.isTableAvailable( "availableTable" ) );
  }

  @Test
  public void testIsTableUnavailable() throws Exception {
    when( hbaseAdminMock.isTableAvailable( "notAvailableTable" ) ).thenReturn( false );
    assertFalse( commonHBaseConnection.isTableAvailable( "notAvailableTable" ) );
  }

  @Test
  public void testDeleteTable() throws Exception {
    commonHBaseConnection.deleteTable( "tableToDelete" );
    verify( hbaseAdminMock ).deleteTable( "tableToDelete" );
  }

  @Test
  public void testGetTableFamiles() throws Exception {
    HTableDescriptor tableDescriptor = mock( HTableDescriptor.class );
    when( tableDescriptor.getFamilies() ).thenReturn( Arrays.asList( new HColumnDescriptor( "family1" ) ) );
    when( hbaseAdminMock.getTableDescriptor( (byte[]) any() ) ).thenReturn( tableDescriptor );

    List<String> families = commonHBaseConnection.getTableFamiles( "tableName" );
    assertEquals( 1, families.size() );
    assertEquals( "family1", families.get( 0 ) );
  }

  @Test
  public void testConfigureColumnDescriptorWithMaxVersions() throws Exception {
    HColumnDescriptor columnDescriptor = mock( HColumnDescriptor.class );
    properties.put( CommonHBaseConnection.COL_DESCRIPTOR_MAX_VERSIONS_KEY, "5" );

    commonHBaseConnection.configureColumnDescriptor( columnDescriptor, properties );
    verify( columnDescriptor ).setMaxVersions( 5 );
  }

  @Test
  public void testConfigureColumnDescriptorInmemory() throws Exception {
    HColumnDescriptor columnDescriptor = mock( HColumnDescriptor.class );
    properties.put( CommonHBaseConnection.COL_DESCRIPTOR_IN_MEMORY_KEY, "no" );
    commonHBaseConnection.configureColumnDescriptor( columnDescriptor, properties );
    verify( columnDescriptor ).setInMemory( false );
  }

  @Test
  public void testConfigureColumnDescriptorBlockCacheEnabledKey() throws Exception {
    HColumnDescriptor columnDescriptor = mock( HColumnDescriptor.class );
    properties.put( CommonHBaseConnection.COL_DESCRIPTOR_BLOCK_CACHE_ENABLED_KEY, "yes" );
    commonHBaseConnection.configureColumnDescriptor( columnDescriptor, properties );
    verify( columnDescriptor ).setBlockCacheEnabled( true );
  }

  @Test
  public void testConfigureColumnDescriptorWithColDescriptorBlockSizeKey() throws Exception {
    HColumnDescriptor columnDescriptor = mock( HColumnDescriptor.class );
    properties.put( CommonHBaseConnection.COL_DESCRIPTOR_BLOCK_SIZE_KEY, "123" );
    commonHBaseConnection.configureColumnDescriptor( columnDescriptor, properties );
    verify( columnDescriptor ).setBlocksize( 123 );
  }

  @Test
  public void testConfigureColumnDescriptorColDescriptorTimeToLiveKey() throws Exception {
    HColumnDescriptor columnDescriptor = mock( HColumnDescriptor.class );
    properties.put( CommonHBaseConnection.COL_DESCRIPTOR_TIME_TO_LIVE_KEY, "1234" );
    commonHBaseConnection.configureColumnDescriptor( columnDescriptor, properties );
    verify( columnDescriptor ).setTimeToLive( 1234 );
  }

  @Test
  public void testConfigureColumnDescriptorColDescriptorScopeKey() throws Exception {
    HColumnDescriptor columnDescriptor = mock( HColumnDescriptor.class );
    properties.put( CommonHBaseConnection.COL_DESCRIPTOR_SCOPE_KEY, "111" );
    commonHBaseConnection.configureColumnDescriptor( columnDescriptor, properties );
    verify( columnDescriptor ).setScope( 111 );
  }

  @Test
  public void testCheckSourceTable() throws Exception {
    thrown.expect( Exception.class );
    thrown.expectMessage( "No source table has been specified" );
    commonHBaseConnection.checkSourceTable();
  }

  @Test
  public void testCheckSourceScan() throws Exception {
    thrown.expect( Exception.class );
    thrown.expectMessage( "No source scan has been defined" );
    commonHBaseConnection.checkSourceScan();
  }

  @Test
  public void testCreateTable() throws Exception {
    HTableDescriptor tableDescriptor = mock( HTableDescriptor.class );
    HBaseClientFactory factory = mock( HBaseClientFactory.class );
    when( factory.getHBaseTableDescriptor( anyString() ) ).thenReturn( tableDescriptor );
    commonHBaseConnection.m_factory = factory;

    commonHBaseConnection.createTable( "name1", Arrays.asList( "columnfamily1", "columnfamily2" ), properties );

    ArgumentCaptor<HColumnDescriptor> columnDescriptorsCaptor = ArgumentCaptor.forClass( HColumnDescriptor.class );
    verify( tableDescriptor, times( 2 ) ).addFamily( columnDescriptorsCaptor.capture() );
    List<HColumnDescriptor> descriptors = columnDescriptorsCaptor.getAllValues();
    assertEquals( descriptors.size(), 2 );
    Assert.assertEquals( descriptors.get( 0 ).getNameAsString(), "columnfamily1" );
    Assert.assertEquals( descriptors.get( 1 ).getNameAsString(), "columnfamily2" );
    verify( hbaseAdminMock ).createTable( tableDescriptor );
  }

  @Test
  public void testNewSourceTable() throws Exception {
    HBaseClientFactory factory = mock( HBaseClientFactory.class );
    commonHBaseConnection.m_factory = factory;

    HBaseTable table = mock( HBaseTable.class );
    commonHBaseConnection.m_sourceTable = table;

    ResultScanner scanner = mock( ResultScanner.class );
    commonHBaseConnection.m_resultSet = scanner;

    commonHBaseConnection.newSourceTable( "tableName1" );

    verify( scanner ).close();
    verify( table ).close();
    verify( factory ).getHBaseTable( "tableName1" );
  }

  @Test
  public void testSourceTableRowExists() throws Exception {
    Result result = mock( Result.class );
    when( result.isEmpty() ).thenReturn( false );

    HBaseTable table = mock( HBaseTable.class );
    when( table.get( (Get) any() ) ).thenReturn( result );
    commonHBaseConnection.m_sourceTable = table;

    boolean exists = commonHBaseConnection.sourceTableRowExists( new byte[] { 1, 2, 3 } );
    assertTrue( exists );
  }

  @Test
  public void testNewSourceTableScan() throws Exception {
    commonHBaseConnection.m_sourceTable = mock( HBaseTable.class );

    commonHBaseConnection.newSourceTableScan( null, null, 0 );
    assertArrayEquals( commonHBaseConnection.m_sourceScan.getStartRow(), new byte[ 0 ] );
    assertArrayEquals( commonHBaseConnection.m_sourceScan.getStopRow(), new byte[ 0 ] );
    assertEquals( commonHBaseConnection.m_sourceScan.getCaching(), -1 );

    commonHBaseConnection.newSourceTableScan( new byte[] { 1, 2 }, null, 100500 );
    assertArrayEquals( commonHBaseConnection.m_sourceScan.getStartRow(), new byte[] { 1, 2 } );
    assertArrayEquals( commonHBaseConnection.m_sourceScan.getStopRow(), new byte[ 0 ] );
    assertEquals( commonHBaseConnection.m_sourceScan.getCaching(), 100500 );

    commonHBaseConnection.newSourceTableScan( new byte[] { 1 }, new byte[] { 2 }, 100501 );
    assertArrayEquals( commonHBaseConnection.m_sourceScan.getStartRow(), new byte[] { 1 } );
    assertArrayEquals( commonHBaseConnection.m_sourceScan.getStopRow(), new byte[] { 2 } );
    assertEquals( commonHBaseConnection.m_sourceScan.getCaching(), 100501 );
  }

  @Test
  public void testAddColumnToScan() throws Exception {
    Scan scan = mock( Scan.class );
    commonHBaseConnection.m_sourceScan = scan;

    HBaseBytesUtilShim bytesUtil = mock( HBaseBytesUtilShim.class );
    commonHBaseConnection.m_bytesUtil = bytesUtil;
    byte[] colFamilyName = new byte[] { 1, 2, 3 };
    byte[] colName = new byte[] { 4, 5, 6 };
    when( bytesUtil.toBytes( "colFamilyName" ) ).thenReturn( colFamilyName );
    when( bytesUtil.toBytes( "colName" ) ).thenReturn( colName );
    commonHBaseConnection.addColumnToScan( "colFamilyName", "colName", false );
    verify( scan ).addColumn( colFamilyName, colName );

    byte[] binaryColFamilyName = new byte[] { 1 };
    byte[] binaryColName = new byte[] { 7 };
    when( bytesUtil.toBytes( "binaryColFamilyName" ) ).thenReturn( binaryColFamilyName );
    when( bytesUtil.toBytesBinary( "binaryColName" ) ).thenReturn( binaryColName );
    commonHBaseConnection.addColumnToScan( "binaryColFamilyName", "binaryColName", true );
    verify( scan ).addColumn( binaryColFamilyName, binaryColName );
  }

  @Test
  public void testAddColumnFilterToScanInteger() throws Exception {
    ColumnFilter cf = new ColumnFilter( "alias" );
    cf.setComparisonOperator( ColumnFilter.ComparisonType.EQUAL );
    cf.setConstant( "1" );

    commonHBaseConnection.m_sourceScan = new Scan();
    VariableSpace space = mockVariableSpace();
    HBaseValueMeta meta = new HBaseValueMeta( "colFamly,colname,alias", 5, 2, 1 );

    commonHBaseConnection.addColumnFilterToScan( cf, meta, space, false );
    FilterList filter = (FilterList) commonHBaseConnection.m_sourceScan.getFilter();
    assertFalse( filter.getFilters().isEmpty() );
    Assert.assertEquals( filter.getFilters().size(), 1 );
  }

  @Test
  public void testAddColumnFilterToScanBooleanUnableToParse() throws Exception {
    ColumnFilter cf = new ColumnFilter( "Family" );
    cf.setComparisonOperator( ColumnFilter.ComparisonType.LESS_THAN );
    cf.setConstant( "not_boolean" );
    cf.setSignedComparison( true );

    VariableSpace space = mockVariableSpace();
    commonHBaseConnection.m_sourceScan = new Scan();
    HBaseValueMeta meta = new HBaseValueMeta( "colFamly,colname,Family", 4, 20, 1 );

    commonHBaseConnection.addColumnFilterToScan( cf, meta, space, true );
    FilterList filter = (FilterList) commonHBaseConnection.m_sourceScan.getFilter();
    assertTrue( filter.getFilters().isEmpty() );
  }

  @Test
  public void testAddColumnFilterToScanCompareOpNull() throws Exception {
    ColumnFilter cf = new ColumnFilter( "Family" );
    cf.setConstant( "123" );
    cf.setSignedComparison( true );

    HBaseValueMeta meta = new HBaseValueMeta( "colFamly,colname,Family", 1, 20, 1 );
    meta.setIsLongOrDouble( true );
    VariableSpace space = mockVariableSpace();
    connectionSpy.m_sourceScan = new Scan();
    doReturn( null ).when( connectionSpy ).getCompareOpByComparisonType( any( ColumnFilter.ComparisonType.class ) );

    connectionSpy.addColumnFilterToScan( cf, meta, space, true );
    FilterList filter = (FilterList) connectionSpy.m_sourceScan.getFilter();
    assertFalse( filter.getFilters().isEmpty() );
    Assert.assertEquals( filter.getFilters().size(), 1 );
    Assert.assertEquals( BinaryPrefixComparator.class,
      ( (CompareFilter) filter.getFilters().get( 0 ) ).getComparator().getClass() );
  }

  @Test
  public void testAddColumnFilterToScanPrefixFilter() throws Exception {
    ColumnFilter cf = new ColumnFilter( "Family" );
    cf.setConstant( "123" );
    cf.setSignedComparison( true );

    VariableSpace space = mockVariableSpace();
    connectionSpy.m_sourceScan = new Scan();
    HBaseValueMeta meta = new HBaseValueMeta( "colFamly,colname,Family", 1, 20, 1 );
    meta.setKey( true );
    meta.setIsLongOrDouble( true );
    doReturn( null ).when( connectionSpy ).getCompareOpByComparisonType( any( ColumnFilter.ComparisonType.class ) );

    connectionSpy.addColumnFilterToScan( cf, meta, space, true );
    FilterList filter = (FilterList) connectionSpy.m_sourceScan.getFilter();
    assertFalse( filter.getFilters().isEmpty() );
    Assert.assertEquals( filter.getFilters().size(), 1 );
    Assert.assertEquals( PrefixFilter.class, filter.getFilters().get( 0 ).getClass() );
  }

  @Test
  public void testCreateEmptyFilterIfNull() throws Exception {
    Scan scan = mock( Scan.class );
    when( scan.getFilter() ).thenReturn( null );
    commonHBaseConnection.m_sourceScan = scan;
    ArgumentCaptor<FilterList> captor = ArgumentCaptor.forClass( FilterList.class );

    commonHBaseConnection.createEmptyFilterIfNull( true );
    commonHBaseConnection.createEmptyFilterIfNull( false );

    verify( scan, times( 2 ) ).setFilter( captor.capture() );
    List<FilterList> allValues = captor.getAllValues();
    Assert.assertEquals( FilterList.Operator.MUST_PASS_ONE, allValues.get( 0 ).getOperator() );
    Assert.assertEquals( FilterList.Operator.MUST_PASS_ALL, allValues.get( 1 ).getOperator() );
  }

  @Test public void testCheckResultSet() throws Exception {
    thrown.expect( Exception.class );
    thrown.expectMessage( "No current result set active" );
    commonHBaseConnection.checkResultSet();
  }

  @Test public void testCheckForCurrentResultSetRow() throws Exception {
    thrown.expect( Exception.class );
    thrown.expectMessage( "No current result active" );
    commonHBaseConnection.checkForCurrentResultSetRow();
  }

  @Test
  public void testExecuteSourceTableScan() throws Exception {
    HBaseTable table = mock( HBaseTable.class );
    commonHBaseConnection.m_sourceTable = table;
    Scan scan = new Scan();
    commonHBaseConnection.m_sourceScan = scan;

    commonHBaseConnection.executeSourceTableScan();
    assertNull( commonHBaseConnection.m_sourceScan.getFilter() );

    scan.setFilter( new FilterList( FilterList.Operator.MUST_PASS_ONE ) );
    commonHBaseConnection.executeSourceTableScan();
    assertNull( commonHBaseConnection.m_sourceScan.getFilter() );

    ResultScanner result = mock( ResultScanner.class );
    when( table.getScanner( scan ) ).thenReturn( result );
    commonHBaseConnection.executeSourceTableScan();
    Assert.assertEquals( result, commonHBaseConnection.m_resultSet );
  }

  @Test
  public void testResultSetNextRow() throws Exception {
    Result result = mock( Result.class );
    ResultScanner resultScanner = mock( ResultScanner.class );
    when( resultScanner.next() ).thenReturn( result );
    commonHBaseConnection.m_resultSet = resultScanner;

    assertTrue( commonHBaseConnection.resultSetNextRow() );

    when( resultScanner.next() ).thenReturn( null );
    assertFalse( commonHBaseConnection.resultSetNextRow() );
  }

  @Test
  public void testCheckForHBaseRow() throws Exception {
    assertTrue( commonHBaseConnection.checkForHBaseRow( new Result() ) );
    assertFalse( commonHBaseConnection.checkForHBaseRow( new Object() ) );
  }

  @Test
  public void testGetRowKey() throws Exception {
    Result result = mock( Result.class );
    when( result.getRow() ).thenReturn( new byte[] { 1, 2, 3 } );
    byte[] rowKey = commonHBaseConnection.getRowKey( result );
    assertArrayEquals( new byte[] { 1, 2, 3 }, rowKey );
  }

  @Test
  public void testGetResultSetCurrentRowKey() throws Exception {
    commonHBaseConnection.m_sourceScan = mock( Scan.class );
    commonHBaseConnection.m_resultSet = mock( ResultScanner.class );

    Result result = mock( Result.class );
    when( result.getRow() ).thenReturn( new byte[] { 1 } );
    commonHBaseConnection.m_currentResultSetRow = result;

    byte[] resultSetCurrentRowKey = commonHBaseConnection.getResultSetCurrentRowKey();
    assertArrayEquals( new byte[] { 1 }, resultSetCurrentRowKey );
  }

  @Test
  public void testGetRowColumnLatest() throws Exception {
    Result aRow = mock( Result.class );
    HBaseBytesUtilShim bytesUtil = commonHBaseConnection.m_bytesUtil;
    when( aRow.getValue( bytesUtil.toBytes( "colFamilyName" ), bytesUtil.toBytes( "colName" ) ) )
      .thenReturn( new byte[] { 1, 2 } );
    byte[] rowColumnLatest = commonHBaseConnection.getRowColumnLatest( aRow, "colFamilyName", "colName", false );
    assertArrayEquals( new byte[] { 1, 2 }, rowColumnLatest );

    when( aRow.getValue( bytesUtil.toBytes( "colFamilyName" ), bytesUtil.toBytesBinary( "colName" ) ) )
      .thenReturn( new byte[] { 1 } );
    byte[] rowColumnLatestBinary = commonHBaseConnection.getRowColumnLatest( aRow, "colFamilyName", "colName", true );
    assertArrayEquals( new byte[] { 1 }, rowColumnLatestBinary );
  }

  @Test
  public void testGetResultSetCurrentRowColumnLatest() throws Exception {
    byte[] familyBytes = new byte[] { 1, 1, 1 };
    byte[] columnBytes = new byte[] { 2, 2, 2 };
    byte[] columnBinaryBytes = new byte[] { 3, 3, 3 };
    byte[] valueBytes = { 1, 2, 3 };
    byte[] valueBinaryBytes = { 4, 5, 6 };

    HBaseBytesUtilShim bytesUtil = mock( HBaseBytesUtilShim.class );
    when( bytesUtil.toBytes( "famName" ) ).thenReturn( familyBytes );
    when( bytesUtil.toBytes( "colName" ) ).thenReturn( columnBytes );
    when( bytesUtil.toBytesBinary( "colNameBinary" ) ).thenReturn( columnBinaryBytes );

    Result resultSet = mock( Result.class );
    when( resultSet.getValue( familyBytes, columnBytes ) ).thenReturn( valueBytes );
    when( resultSet.getValue( familyBytes, columnBinaryBytes ) ).thenReturn( valueBinaryBytes );

    commonHBaseConnection.m_bytesUtil = bytesUtil;
    commonHBaseConnection.m_currentResultSetRow = resultSet;
    commonHBaseConnection.m_sourceScan = mock( Scan.class );
    commonHBaseConnection.m_resultSet = mock( ResultScanner.class );

    byte[] result = commonHBaseConnection.getResultSetCurrentRowColumnLatest( "famName", "colName", false );
    assertArrayEquals( valueBytes, result );

    byte[] resultBinary = commonHBaseConnection.getResultSetCurrentRowColumnLatest( "famName", "colNameBinary", true );
    assertArrayEquals( valueBinaryBytes, resultBinary );
  }

  @Test
  public void testGetRowFamilyMapException() throws Exception {
    thrown.expect( Exception.class );
    thrown.expectMessage( "The supplied object is not an HBase row object" );
    commonHBaseConnection.getRowFamilyMap( new Object(), "familyName" );
  }

  @Test
  public void testGetRowFamilyMap() throws Exception {
    NavigableMap<byte[], byte[]> map = mock( NavigableMap.class );
    Result result = createResultMockForFamilyName( "familyName", map );
    NavigableMap<byte[], byte[]> familyName = commonHBaseConnection.getRowFamilyMap( result, "familyName" );
    assertEquals( map, familyName );
  }

  private Result createResultMockForFamilyName( String familyName, NavigableMap<byte[], byte[]> map ) {
    CommonHBaseBytesUtil util = new CommonHBaseBytesUtil();
    byte[] bytes = util.toBytes( familyName );
    Result result = mock( Result.class );
    when( result.getFamilyMap( bytes ) ).thenReturn( map );
    return result;
  }

  @Test
  public void testGetResultSetCurrentRowFamilyMap() throws Exception {
    NavigableMap<byte[], byte[]> map = mock( NavigableMap.class );
    commonHBaseConnection.m_currentResultSetRow = createResultMockForFamilyName( "familyName", map );
    commonHBaseConnection.m_sourceScan = new Scan();
    commonHBaseConnection.m_resultSet = mock( ResultScanner.class );

    NavigableMap<byte[], byte[]> familyName = commonHBaseConnection.getResultSetCurrentRowFamilyMap( "familyName" );
    assertEquals( map, familyName );
  }

  @Test
  public void testGetRowMapException() throws Exception {
    thrown.expect( Exception.class );
    thrown.expectMessage( "The supplied object is not an HBase row object" );
    commonHBaseConnection.getRowMap( new Object() );
  }

  @Test
  public void testGetRowMap() throws Exception {
    NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = mock( NavigableMap.class );
    Result result = mock( Result.class );
    when( result.getMap() ).thenReturn( map );
    assertEquals( map, commonHBaseConnection.getRowMap( result ) );
  }

  @Test
  public void testGetResultSetCurrentRowMap() throws Exception {
    NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = mock( NavigableMap.class );
    Result result = mock( Result.class );
    when( result.getMap() ).thenReturn( map );
    commonHBaseConnection.m_sourceScan = mock( Scan.class );
    commonHBaseConnection.m_resultSet = mock( ResultScanner.class );
    commonHBaseConnection.m_currentResultSetRow = result;
    assertEquals( map, commonHBaseConnection.getResultSetCurrentRowMap() );
  }

  @Test
  public void testCheckTargetTable() throws Exception {
    thrown.expect( Exception.class );
    thrown.expectMessage( "No target table has been specified" );
    commonHBaseConnection.checkTargetTable();
  }

  @Test
  public void testCheckTargetPut() throws Exception {
    thrown.expect( Exception.class );
    thrown.expectMessage( "No target table put has been specified" );
    commonHBaseConnection.checkTargetPut();
  }

  @Test
  public void testNewTargetTable() throws Exception {
    HBaseTable tableMock = mock( HBaseTable.class );
    HBaseClientFactory factoryMock = mock( HBaseClientFactory.class );
    when( factoryMock.getHBaseTable( "tableName" ) ).thenReturn( tableMock );
    commonHBaseConnection.m_factory = factoryMock;
    properties.put( "htable.writeBufferSize", "123" );

    commonHBaseConnection.newTargetTable( "tableName", properties );
    verify( tableMock ).setWriteBufferSize( 123 );
    verify( tableMock ).setAutoFlush( false );
  }

  @Test
  public void testTargetTableIsAutoFlushException() throws Exception {
    thrown.expect( Exception.class );
    thrown.expectMessage( "No target table has been specified" );
    commonHBaseConnection.targetTableIsAutoFlush();
  }

  @Test
  public void testTargetTableIsAutoFlush() throws Exception {
    HBaseTable table = mock( HBaseTable.class );
    commonHBaseConnection.m_targetTable = table;

    when( table.isAutoFlush() ).thenReturn( true );
    assertTrue( commonHBaseConnection.targetTableIsAutoFlush() );

    when( table.isAutoFlush() ).thenReturn( false );
    assertFalse( commonHBaseConnection.targetTableIsAutoFlush() );
  }


  @Test
  public void testNewTargetTablePut() throws Exception {
    byte[] key = new byte[] { 1, 2, 3 };
    HBaseClientFactory factoryMock = mock( HBaseClientFactory.class );
    HBasePut hbasePut = mock( HBasePut.class );
    when( factoryMock.getHBasePut( key ) ).thenReturn( hbasePut );
    commonHBaseConnection.m_factory = factoryMock;
    commonHBaseConnection.m_targetTable = mock( HBaseTable.class );

    commonHBaseConnection.newTargetTablePut( key, true );
    verify( hbasePut ).setWriteToWAL( true );
  }

  @Test
  public void testExecuteTargetTablePut() throws Exception {
    HBaseTable tableMock = mock( HBaseTable.class );
    HBasePut putMock = mock( HBasePut.class );
    commonHBaseConnection.m_targetTable = tableMock;
    commonHBaseConnection.m_currentTargetPut = putMock;

    commonHBaseConnection.executeTargetTablePut();
    verify( tableMock ).put( putMock );
  }

  @Test
  public void testExecuteTargetTableDelete() throws Exception {
    HBaseTable table = mock( HBaseTable.class );
    commonHBaseConnection.m_targetTable = table;
    ArgumentCaptor<Delete> captor = ArgumentCaptor.forClass( Delete.class );

    commonHBaseConnection.executeTargetTableDelete( new byte[] { 7, 7, 7 } );
    verify( table ).delete( captor.capture() );
    Assert.assertArrayEquals( new byte[] { 7, 7, 7 }, captor.getValue().getRow() );
  }

  @Test
  public void testFlushCommitsTargetTable() throws Exception {
    HBaseTable tableMock = mock( HBaseTable.class );
    commonHBaseConnection.m_targetTable = tableMock;

    commonHBaseConnection.flushCommitsTargetTable();
    verify( tableMock ).flushCommits();
  }

  @Test
  public void testAddColumnToTargetPut() throws Exception {
    commonHBaseConnection.m_targetTable = mock( HBaseTable.class );
    HBasePut targetPutMock = mock( HBasePut.class );
    commonHBaseConnection.m_currentTargetPut = targetPutMock;

    byte[] value = new byte[] { 1, 2, 3 };
    byte[] colFamilies = Bytes.toBytes( "colFamily" );
    byte[] colNames = Bytes.toBytes( "colName" );
    byte[] colNamesBinary = Bytes.toBytesBinary( "colNameBinary" );

    HBaseBytesUtilShim mock = mock( HBaseBytesUtilShim.class );
    when( mock.toBytes( "colFamily" ) ).thenReturn( colFamilies );
    when( mock.toBytes( "colName" ) ).thenReturn( colNames );
    when( mock.toBytesBinary( "colNameBinary" ) ).thenReturn( colNamesBinary );
    commonHBaseConnection.m_bytesUtil = mock;

    commonHBaseConnection.addColumnToTargetPut( "colFamily", "colName", false, value );
    verify( targetPutMock ).addColumn( colFamilies, colNames, value );

    commonHBaseConnection.addColumnToTargetPut( "colFamily", "colNameBinary", true, value );
    verify( targetPutMock ).addColumn( colFamilies, colNamesBinary, value );
  }

  @Test
  public void testCloseTargetTable() throws Exception {
    HBaseTable tableMock = mock( HBaseTable.class );
    commonHBaseConnection.m_targetTable = tableMock;
    when( tableMock.isAutoFlush() ).thenReturn( false );

    commonHBaseConnection.closeTargetTable();

    verify( tableMock ).flushCommits();
    verify( tableMock ).close();
    assertNull( commonHBaseConnection.m_targetTable );
  }

  @Test
  public void testCloseSourceResultSet() throws Exception {
    ResultScanner resultScanner = mock( ResultScanner.class );
    commonHBaseConnection.m_resultSet = resultScanner;

    commonHBaseConnection.closeSourceResultSet();

    verify( resultScanner ).close();
    assertNull( commonHBaseConnection.m_resultSet );
    assertNull( commonHBaseConnection.m_currentResultSetRow );
  }

  @Test
  public void testCloseSourceTable() throws Exception {
    HBaseTable tableMock = mock( HBaseTable.class );
    commonHBaseConnection.m_sourceTable = tableMock;

    commonHBaseConnection.closeSourceTable();
    verify( tableMock ).close();
    assertNull( commonHBaseConnection.m_sourceTable );
  }

  @Test
  public void testIsImmutableBytesWritable() throws Exception {
    assertFalse( commonHBaseConnection.isImmutableBytesWritable( new Object() ) );
    assertTrue( commonHBaseConnection.isImmutableBytesWritable( new ImmutableBytesWritable() ) );
  }

  @Test
  public void testClose() throws Exception {
    HBaseTable targetTable = mock( HBaseTable.class );
    commonHBaseConnection.m_targetTable = targetTable;

    ResultScanner resultSet = mock( ResultScanner.class );
    commonHBaseConnection.m_resultSet = resultSet;

    HBaseTable sourceTable = mock( HBaseTable.class );
    commonHBaseConnection.m_sourceTable = sourceTable;

    HBaseClientFactory factory = mock( HBaseClientFactory.class );
    commonHBaseConnection.m_factory = factory;

    commonHBaseConnection.close();

    verify( targetTable ).close();
    assertNull( commonHBaseConnection.m_targetTable );
    verify( resultSet ).close();
    assertNull( commonHBaseConnection.m_resultSet );
    verify( sourceTable ).close();
    assertNull( commonHBaseConnection.m_sourceTable );
    verify( factory ).close();
    assertNull( commonHBaseConnection.m_factory );
  }

  @Test
  public void testCloseClientFactory() throws Exception {
    HBaseClientFactory factory = mock( HBaseClientFactory.class );
    commonHBaseConnection.m_factory = factory;

    commonHBaseConnection.closeClientFactory();
    verify( factory ).close();
    assertNull( commonHBaseConnection.m_factory );
  }

  @Test
  public void testToBoolean() throws Exception {
    assertTrue( commonHBaseConnection.toBoolean( "Y" ) );
    assertTrue( commonHBaseConnection.toBoolean( "y" ) );
    assertTrue( commonHBaseConnection.toBoolean( "Yes" ) );
    assertTrue( commonHBaseConnection.toBoolean( "yes" ) );
    assertTrue( commonHBaseConnection.toBoolean( "tRuE" ) );
    assertTrue( commonHBaseConnection.toBoolean( "true" ) );
    assertFalse( commonHBaseConnection.toBoolean( "false" ) );
  }

  @Test
  public void getSignerComparisonComparatorIntLong() throws Exception {
    doReturn( FakeDeserializedNumericComparator.class ).when( connectionSpy ).getDeserializedNumericComparatorClass();
    HBaseValueMeta meta = mock( HBaseValueMeta.class );
    when( meta.isInteger() ).thenReturn( true );
    when( meta.getIsLongOrDouble() ).thenReturn( true );

    Number number = mock( Number.class );
    when( number.longValue() ).thenReturn( 5L );

    FakeDeserializedNumericComparator
      signedComparisonComparator =
      (FakeDeserializedNumericComparator) connectionSpy.getSignedComparisonComparator( meta, number );
    assertEquals( true, signedComparisonComparator.isInteger );
    assertEquals( true, signedComparisonComparator.isLongOrDouble );
    assertEquals( 5, signedComparisonComparator.longValue );
    assertEquals( 0, signedComparisonComparator.doubleValue, 0 );
  }

  @Test
  public void getSignerComparisonComparatorInt() throws Exception {
    doReturn( FakeDeserializedNumericComparator.class ).when( connectionSpy ).getDeserializedNumericComparatorClass();
    HBaseValueMeta meta = mock( HBaseValueMeta.class );
    when( meta.isInteger() ).thenReturn( true );
    when( meta.getIsLongOrDouble() ).thenReturn( false );

    Number number = mock( Number.class );
    when( number.intValue() ).thenReturn( 1 );

    FakeDeserializedNumericComparator
      signedComparisonComparator =
      (FakeDeserializedNumericComparator) connectionSpy.getSignedComparisonComparator( meta, number );
    assertEquals( true, signedComparisonComparator.isInteger );
    assertEquals( false, signedComparisonComparator.isLongOrDouble );
    assertEquals( 1, signedComparisonComparator.longValue );
  }

  @Test
  public void getSignerComparisonComparatorDouble() throws Exception {
    doReturn( FakeDeserializedNumericComparator.class ).when( connectionSpy ).getDeserializedNumericComparatorClass();
    HBaseValueMeta meta = mock( HBaseValueMeta.class );
    when( meta.isInteger() ).thenReturn( false );
    when( meta.getIsLongOrDouble() ).thenReturn( true );

    Number number = mock( Number.class );
    when( number.doubleValue() ).thenReturn( 25D );

    FakeDeserializedNumericComparator
      signedComparisonComparator =
      (FakeDeserializedNumericComparator) connectionSpy.getSignedComparisonComparator( meta, number );
    assertEquals( false, signedComparisonComparator.isInteger );
    assertEquals( true, signedComparisonComparator.isLongOrDouble );
    assertEquals( 25D, signedComparisonComparator.doubleValue, 0 );
  }

  @Test
  public void getSignerComparisonComparatorFloat() throws Exception {
    doReturn( FakeDeserializedNumericComparator.class ).when( connectionSpy ).getDeserializedNumericComparatorClass();
    HBaseValueMeta meta = mock( HBaseValueMeta.class );
    when( meta.isInteger() ).thenReturn( false );
    when( meta.getIsLongOrDouble() ).thenReturn( false );

    Number number = mock( Number.class );
    when( number.floatValue() ).thenReturn( 100F );

    FakeDeserializedNumericComparator
      signedComparisonComparator =
      (FakeDeserializedNumericComparator) connectionSpy.getSignedComparisonComparator( meta, number );
    assertEquals( false, signedComparisonComparator.isInteger );
    assertEquals( false, signedComparisonComparator.isLongOrDouble );
    assertEquals( 100F, signedComparisonComparator.doubleValue, 0 );
  }

  @Test
  public void testGetBooleanComparator() throws Exception {
    doReturn( FakeDeserializedBooleanComparator.class ).when( connectionSpy ).getDeserializedBooleanComparatorClass();
    FakeDeserializedBooleanComparator
      comparator =
      (FakeDeserializedBooleanComparator) connectionSpy.getBooleanComparator( Boolean.FALSE );
    assertFalse( comparator.value );
    comparator = (FakeDeserializedBooleanComparator) connectionSpy.getBooleanComparator( Boolean.TRUE );
    assertTrue( comparator.value );
  }

  @Test
  public void testGetDateComparatorByte() throws Exception {
    ColumnFilter cf = mock( ColumnFilter.class );
    when( cf.getSignedComparison() ).thenReturn( false );
    VariableSpace space = mockVariableSpace();
    Object dateComparator = commonHBaseConnection.getDateComparator( cf, space, "10/10/96 4:5 PM" );
    assertEquals( byte[].class, dateComparator.getClass() );
  }

  @Test
  public void testGetDateComparator() throws Exception {
    ColumnFilter cf = mock( ColumnFilter.class );
    when( cf.getSignedComparison() ).thenReturn( true );
    VariableSpace space = mockVariableSpace();
    doReturn( FakeDeserializedNumericComparator.class ).when( connectionSpy ).getDeserializedNumericComparatorClass();

    Object dateComparator = connectionSpy.getDateComparator( cf, space, "10/10/96 4:5 PM" );
    assertEquals( FakeDeserializedNumericComparator.class, dateComparator.getClass() );
    long timeExpected = new SimpleDateFormat().parse( "10/10/96 4:5 PM" ).getTime();
    assertEquals( timeExpected, ( (FakeDeserializedNumericComparator) dateComparator ).longValue );
  }

  @Test
  public void testGetCompareOpByComparisonType() throws Exception {
    Assert.assertEquals( CompareFilter.CompareOp.EQUAL,
      commonHBaseConnection.getCompareOpByComparisonType( ColumnFilter.ComparisonType.EQUAL ) );
    Assert.assertEquals( CompareFilter.CompareOp.NOT_EQUAL,
      commonHBaseConnection.getCompareOpByComparisonType( ColumnFilter.ComparisonType.NOT_EQUAL ) );
    Assert.assertEquals( CompareFilter.CompareOp.GREATER,
      commonHBaseConnection.getCompareOpByComparisonType( ColumnFilter.ComparisonType.GREATER_THAN ) );
    Assert.assertEquals( CompareFilter.CompareOp.GREATER_OR_EQUAL,
      commonHBaseConnection.getCompareOpByComparisonType( ColumnFilter.ComparisonType.GREATER_THAN_OR_EQUAL ) );
    Assert.assertEquals( CompareFilter.CompareOp.LESS,
      commonHBaseConnection.getCompareOpByComparisonType( ColumnFilter.ComparisonType.LESS_THAN ) );
    Assert.assertEquals( CompareFilter.CompareOp.LESS_OR_EQUAL,
      commonHBaseConnection.getCompareOpByComparisonType( ColumnFilter.ComparisonType.LESS_THAN_OR_EQUAL ) );
    assertNull( commonHBaseConnection.getCompareOpByComparisonType( ColumnFilter.ComparisonType.PREFIX ) );
  }

  private void useMockForHBaseClientFactory() {
    doReturn( mock( HBaseClientFactory.class ) ).when( connectionSpy )
      .getHBaseClientFactory( any( Configuration.class ) );
  }

  public static VariableSpace mockVariableSpace() {
    VariableSpace space = mock( VariableSpace.class );
    when( space.environmentSubstitute( anyString() ) ).thenAnswer( new Answer<String>() {
      @Override
      public String answer( InvocationOnMock invocation ) throws Throwable {
        return (String) invocation.getArguments()[ 0 ];
      }
    } );
    return space;
  }

  private static class FakeDeserializedNumericComparator {
    byte[] raw;
    boolean isInteger;
    boolean isLongOrDouble;
    long longValue;
    double doubleValue;

    public FakeDeserializedNumericComparator( byte[] raw ) {
      this.raw = raw;
    }

    public FakeDeserializedNumericComparator( boolean isInteger, boolean isLongOrDouble, long value ) {
      this.isInteger = isInteger;
      this.isLongOrDouble = isLongOrDouble;
      this.longValue = value;
    }

    public FakeDeserializedNumericComparator( boolean isInteger, boolean isLongOrDouble, double value ) {
      this.isInteger = isInteger;
      this.isLongOrDouble = isLongOrDouble;
      doubleValue = value;
    }
  }

  private static class FakeDeserializedBooleanComparator {
    boolean value;

    public FakeDeserializedBooleanComparator( byte[] raw ) {
      value = Bytes.toBoolean( raw );
    }

    public FakeDeserializedBooleanComparator( boolean value ) {
      this.value = value;
    }
  }

  @AfterClass
  public static void tearDown() {
    KettleLogStore.setLogChannelInterfaceFactory( oldLogChannelInterfaceFactory );
  }

}
