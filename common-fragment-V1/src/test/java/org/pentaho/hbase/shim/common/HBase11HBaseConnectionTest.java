/**
 * ****************************************************************************
 * <p>
 * Pentaho Big Data
 * <p>
 * Copyright (C) 2016 - 2019 by Hitachi Vantara : http://www.pentaho.com
 * <p>
 * ******************************************************************************
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 * <p>
 * ****************************************************************************
 */

package org.pentaho.hbase.shim.common;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.pentaho.di.core.logging.KettleLogStore;
import org.pentaho.di.core.logging.LogChannelInterfaceFactory;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.hbase.factory.HBaseAdmin;

import java.util.List;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class HBase11HBaseConnectionTest {
  //   private CommonHBaseConnection commonHBaseConnection;
  //   private CommonHBaseConnection connectionSpy;
  //   private static LogChannelInterfaceFactory oldLogChannelInterfaceFactory;
  //
  //   @Rule
  //   public ExpectedException thrown = ExpectedException.none();
  //   private Properties properties;
  //   private HBaseAdmin hbaseAdminMock;
  //
  //   @BeforeClass
  //   public static void beforeClass() throws Exception {
  //     oldLogChannelInterfaceFactory = KettleLogStore.getLogChannelInterfaceFactory();
  //     CommonHBaseConnectionTest.setKettleLogFactoryWithMock();
  //   }
  //
  //   @Before
  //   public void setUp() throws Exception {
  //     commonHBaseConnection = new HBaseConnectionImpl();
  //     hbaseAdminMock = mock( HBaseAdmin.class );
  //     commonHBaseConnection.m_admin = hbaseAdminMock;
  //     connectionSpy = Mockito.spy( commonHBaseConnection );
  //     properties = new Properties();
  //   }
  //
  //   @Test
  //   public void testListTableNames() throws Exception {
  //     when( hbaseAdminMock.listTables() )
  //       .thenReturn( new HTableDescriptor[] { new HTableDescriptor( TableName.valueOf( "test" ) ) } );
  //
  //     List<String> tableNames = commonHBaseConnection.listTableNames();
  //
  //     assertThat( tableNames.size(), is( 1 ) );
  //     assertThat( tableNames.get( 0 ), is( "test" ) );
  //   }
  //
  //   @Test
  //   public void testAddColumnFilterToScanDate() throws Exception {
  //     ColumnFilter cf = new ColumnFilter( "Family" );
  //     cf.setComparisonOperator( ColumnFilter.ComparisonType.LESS_THAN );
  //     cf.setConstant( "07/10/96 4:5 PM" );
  //     cf.setSignedComparison( true );
  //
  //     VariableSpace space = CommonHBaseConnectionTest.mockVariableSpace();
  //     connectionSpy.m_sourceScan = new Scan();
  //     ByteArrayComparable comparator = mock( ByteArrayComparable.class );
  //     doReturn( comparator ).when( connectionSpy ).getDateComparator( eq( cf ), eq( space ), anyString() );
  //     HBaseValueMeta meta = new HBaseValueMeta( "colFamly,colname,Family", 3, 20, 1 );
  //
  //     connectionSpy.addColumnFilterToScan( cf, meta, space, true );
  //     FilterList filter = (FilterList) connectionSpy.m_sourceScan.getFilter();
  //     assertFalse( filter.getFilters().isEmpty() );
  //     assertEquals( filter.getFilters().size(), 1 );
  //   }
  //
  //   @Test
  //   public void testAddColumnFilterToScanBoolean() throws Exception {
  //     ColumnFilter cf = new ColumnFilter( "Family" );
  //     cf.setComparisonOperator( ColumnFilter.ComparisonType.LESS_THAN );
  //     cf.setConstant( "true" );
  //     cf.setSignedComparison( true );
  //
  //     VariableSpace space = CommonHBaseConnectionTest.mockVariableSpace();
  //     connectionSpy.m_sourceScan = new Scan();
  //     ByteArrayComparable comparator = mock( ByteArrayComparable.class );
  //     doReturn( comparator ).when( connectionSpy ).getBooleanComparator( anyBoolean() );
  //     HBaseValueMeta meta = new HBaseValueMeta( "colFamly,colname,Family", 4, 20, 1 );
  //
  //     connectionSpy.addColumnFilterToScan( cf, meta, space, true );
  //     FilterList filter = (FilterList) connectionSpy.m_sourceScan.getFilter();
  //     assertFalse( filter.getFilters().isEmpty() );
  //     assertEquals( filter.getFilters().size(), 1 );
  //   }
  //
  //   @Test
  //   public void testAddColumnFilterToScanNumberSigned() throws Exception {
  //     ColumnFilter cf = new ColumnFilter( "Family" );
  //     cf.setComparisonOperator( ColumnFilter.ComparisonType.LESS_THAN );
  //     cf.setConstant( "123" );
  //     cf.setSignedComparison( true );
  //
  //     VariableSpace space = CommonHBaseConnectionTest.mockVariableSpace();
  //     connectionSpy.m_sourceScan = new Scan();
  //     HBaseValueMeta meta = new HBaseValueMeta( "colFamly,colname,Family", 1, 20, 1 );
  //     meta.setIsLongOrDouble( true );
  //     ByteArrayComparable comparator = mock( ByteArrayComparable.class );
  //     doReturn( comparator ).when( connectionSpy ).getNumericComparator( eq( cf ), eq( meta ), eq( space ),
  //     anyString() );
  //
  //     connectionSpy.addColumnFilterToScan( cf, meta, space, true );
  //     FilterList filter = (FilterList) connectionSpy.m_sourceScan.getFilter();
  //     assertFalse( filter.getFilters().isEmpty() );
  //     assertEquals( filter.getFilters().size(), 1 );
  //   }
  //
  //   @Test
  //   public void testAddFilterByMapping() throws Exception {
  //     testAddFilterByMapping( Mapping.TupleMapping.KEY, RowFilter.class );
  //     testAddFilterByMapping( Mapping.TupleMapping.FAMILY, FamilyFilter.class );
  //     testAddFilterByMapping( Mapping.TupleMapping.COLUMN, QualifierFilter.class );
  //     testAddFilterByMapping( Mapping.TupleMapping.VALUE, ValueFilter.class );
  //   }
  //
  //   private <T> void testAddFilterByMapping( Mapping.TupleMapping mapping, Class<T> filterClass ) throws Exception {
  //     FilterList list = new FilterList( FilterList.Operator.MUST_PASS_ONE );
  //     ByteArrayComparable comparator = new BinaryComparator( new byte[] { 1, 1, 1 } );
  //
  //     commonHBaseConnection.addFilterByMapping( list, CompareFilter.CompareOp.EQUAL, ByteArrayComparable.class,
  //       comparator, mapping );
  //     assertEquals( 1, list.getFilters().size() );
  //     assertEquals( filterClass, list.getFilters().get( 0 ).getClass() );
  //   }
  //
  //   @Test
  //   public void testGetByteArrayComparableClass() throws Exception {
  //     Class<?> byteArrayComparableClass = commonHBaseConnection.getByteArrayComparableClass();
  //     assertEquals( "org.apache.hadoop.hbase.filter.ByteArrayComparable", byteArrayComparableClass.getName() );
  //   }
  //
  //   @Test
  //   public void testGetCompressionAlgorithmClass() throws Exception {
  //     Class<?> compressionAlgorithmClass = commonHBaseConnection.getCompressionAlgorithmClass();
  //     assertEquals( "org.apache.hadoop.hbase.io.compress.Compression$Algorithm", compressionAlgorithmClass.getName
  //     () );
  //   }
  //
  //   @Test
  //   public void testGetBloomTypeClass() throws Exception {
  //     Class<?> bloomTypeClass = commonHBaseConnection.getBloomTypeClass();
  //     assertEquals( "org.apache.hadoop.hbase.regionserver.BloomType", bloomTypeClass.getName() );
  //   }
  //
  //   @Test
  //   public void testConfigureColumnDescriptorWithColDescriptorCompressionKey() throws Exception {
  //     doReturn( Class.forName( "org.apache.hadoop.hbase.io.compress.Compression$Algorithm" ) )
  //       .when( connectionSpy ).getCompressionAlgorithmClass();
  //     HColumnDescriptor columnDescriptor = mock( HColumnDescriptor.class );
  //     properties.put( CommonHBaseConnection.COL_DESCRIPTOR_COMPRESSION_KEY, "LZO" );
  //     connectionSpy.configureColumnDescriptor( columnDescriptor, properties );
  //     verify( columnDescriptor ).setCompressionType( Matchers.<Compression.Algorithm>anyObject() );
  //   }
  //
  //   @AfterClass
  //   public static void tearDown() {
  //     KettleLogStore.setLogChannelInterfaceFactory( oldLogChannelInterfaceFactory );
  //   }
  //
}
