/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2016 by Pentaho : http://www.pentaho.com
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

package org.pentaho.hbase.shim.api;

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilderFactory;
import java.io.ByteArrayInputStream;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.mockito.Matchers;
import org.mockito.Mockito;

/**
 * User: Dzmitry Stsiapanau Date: 10/16/2015 Time: 08:38
 */

public class MappingTest {

  public static final String XML_NODE = "<mapping>      <mapping_name>mapping_name</mapping_name>      <table_name>table_name</table_name>      <key>key</key>      <key_type>String</key_type>        <mapped_columns>        <mapped_column>          <alias>alias_2</alias>          <column_family>col_family_2</column_family>          <column_name>col_name_2</column_name>          <type>String</type>        </mapped_column>        <mapped_column>          <alias>alias_1</alias>          <column_family>col_family_1</column_family>          <column_name>col_name_1</column_name>          <type>Double</type>        </mapped_column>        </mapped_columns>    </mapping>";
  private static final String MAPPING_NAME = "mapping_name";
  private static final String TABLE_NAME = "table_name";
  private static final String KEY = "key";
  private static final Mapping.KeyType KEY_TYPE = Mapping.KeyType.STRING;
  private static final String[] ALIAS = new String[] { "alias_2", "alias_1" };
  private static final String[] COLUMN_FAMILY = new String[] { "col_family_2", "col_family_1" };
  private static final String[] COLUMN_NAME = new String[] { "col_name_2", "col_name_1" };
  private static final String[] TYPE = new String[] { "String", "Double" };
  private static final String[] INDEXED_VALS = new String[] { "", "" };

  private Mapping getMapping() throws Exception {
    Mapping mapping = new Mapping();
    mapping.setMappingName( MAPPING_NAME );
    mapping.setTableName( TABLE_NAME );
    mapping.setKeyName( KEY );
    mapping.setKeyType( KEY_TYPE );
    mapping.addMappedColumn( new HBaseValueMeta( "col_family_1,col_name_1,alias_1", 1, 0, 0 ), false );
    mapping.addMappedColumn( new HBaseValueMeta( "col_family_2,col_name_2,alias_2", 2, 0, 0 ), false );
    return mapping;
  }

  private Mapping getMappingWithAllTypes() throws Exception {
    Mapping mapping = new Mapping();
    HBaseValueMeta hBaseValueMeta;
    mapping.setMappingName( MAPPING_NAME );
    mapping.setTableName( TABLE_NAME );
    mapping.setKeyName( KEY );
    mapping.setKeyType( KEY_TYPE );
    mapping.addMappedColumn( new HBaseValueMeta( "column_family_1,LongColumnName,alias_1", 5, 0, 0 ), false );
    hBaseValueMeta = new HBaseValueMeta( "column_family_2,IntegerColumnName,alias_2", 5, 0, 0 );
    hBaseValueMeta.setIsLongOrDouble( false );
    mapping.addMappedColumn( hBaseValueMeta, false );
    mapping.addMappedColumn( new HBaseValueMeta( "column_family_4,DoubleColumnName,alias_4", 1, 0, 0 ), false );
    hBaseValueMeta = new HBaseValueMeta( "column_family_3,FloatColumnName,alias_3", 1, 0, 0 );
    hBaseValueMeta.setIsLongOrDouble( false );
    mapping.addMappedColumn( hBaseValueMeta, false );
    mapping.addMappedColumn( new HBaseValueMeta( "col_family_5,DateColumnName,alias_5", 3, 0, 0 ), false  );
    mapping.addMappedColumn( new HBaseValueMeta( "col_family_6,SerializableColumnName,alias_6", 7, 0, 0 ), false  );
    mapping.addMappedColumn( new HBaseValueMeta( "col_family_7,BooleanColumnName,alias_7", 4, 0, 0 ), false  );
    mapping.addMappedColumn( new HBaseValueMeta( "col_family_8,BinaryColumnName,alias_8", 8, 0, 0 ), false  );
    mapping.addMappedColumn( new HBaseValueMeta( "col_family_9,BigNumberColumnName,alias_9", 6, 0, 0 ), false  );
    mapping.addMappedColumn( new HBaseValueMeta( "col_family_10,StringColumnName,alias_10", 2, 0, 0 ), false  );

    return mapping;
  }

  @Test
  public void testAddMappedColumn() throws Exception {
    Mapping mapping = new Mapping();
    HBaseValueMeta hbaseValueMeta = new HBaseValueMeta( "col_family,col_name,alias", 0, 0, 0 );
    HBaseValueMeta hbaseValueMeta2 = new HBaseValueMeta( "col_family,col_name2,alias", 0, 0, 0 );
    HBaseValueMeta hbaseValueMeta3 = new HBaseValueMeta( "col_family,col_name3,alias", 0, 0, 0 );
    Assert.assertEquals( "alias", mapping.addMappedColumn( hbaseValueMeta, false ) );
    Assert.assertEquals( 1, mapping.m_mappedColumnsByFamilyCol.size() );
    try {
      mapping.addMappedColumn( hbaseValueMeta, false );
      Assert.fail( "the same column" );
    } catch ( Exception e ) {
      //ignored
    }
    Assert.assertEquals( "alias_1", mapping.addMappedColumn( hbaseValueMeta2, false ) );
    Assert.assertEquals( 2, mapping.m_mappedColumnsByFamilyCol.size() );

    Assert.assertEquals( "alias_1", mapping.addMappedColumn( hbaseValueMeta3, true ) );
    Assert.assertEquals( 2, mapping.m_mappedColumnsByFamilyCol.size() );
  }

  @Test
  public void testSetTableName() throws Exception {
    Mapping mapping = getMapping();
    mapping.setTableName( "test" );
    Assert.assertEquals( "test", mapping.m_tableName );
  }

  @Test
  public void testGetTableName() throws Exception {
    Mapping mapping = getMapping();
    mapping.m_tableName = "test";
    Assert.assertEquals( "test", mapping.getTableName() );
  }

  @Test
  public void testSetMappingName() throws Exception {
    Mapping mapping = getMapping();
    mapping.setMappingName( "test" );
    Assert.assertEquals( "test", mapping.m_mappingName );
  }

  @Test
  public void testGetMappingName() throws Exception {
    Mapping mapping = getMapping();
    mapping.m_mappingName = "test";
    Assert.assertEquals( "test", mapping.getMappingName() );
  }

  @Test
  public void testSetKeyName() throws Exception {
    Mapping mapping = getMapping();
    mapping.setKeyName( "test" );
    Assert.assertEquals( "test", mapping.m_keyName );
  }

  @Test
  public void testGetKeyName() throws Exception {
    Mapping mapping = getMapping();
    mapping.m_keyName = "test";
    Assert.assertEquals( "test", mapping.getKeyName() );
  }

  @Test
  public void testSetKeyType() throws Exception {
    Mapping mapping = getMapping();
    mapping.setKeyType( Mapping.KeyType.STRING );
    Assert.assertEquals( Mapping.KeyType.STRING, mapping.m_keyType );
  }

  @Test
  public void testSetKeyTypeAsString() throws Exception {
    Mapping mapping = getMapping();
    mapping.setKeyTypeAsString( "UnsignedLong" );
    Assert.assertEquals( Mapping.KeyType.UNSIGNED_LONG, mapping.m_keyType );
    mapping.setKeyTypeAsString( "Binary" );
    Assert.assertEquals( Mapping.KeyType.BINARY, mapping.m_keyType );
    mapping.setKeyTypeAsString( "Date" );
    Assert.assertEquals( Mapping.KeyType.DATE, mapping.m_keyType );
    mapping.setKeyTypeAsString( "Integer" );
    Assert.assertEquals( Mapping.KeyType.INTEGER, mapping.m_keyType );
    mapping.setKeyTypeAsString( "Long" );
    Assert.assertEquals( Mapping.KeyType.LONG, mapping.m_keyType );
    mapping.setKeyTypeAsString( "String" );
    Assert.assertEquals( Mapping.KeyType.STRING, mapping.m_keyType );
    mapping.setKeyTypeAsString( "UnsignedDate" );
    Assert.assertEquals( Mapping.KeyType.UNSIGNED_DATE, mapping.m_keyType );
    mapping.setKeyTypeAsString( "UnsignedInteger" );
    Assert.assertEquals( Mapping.KeyType.UNSIGNED_INTEGER, mapping.m_keyType );
  }

  @Test
  public void testGetKeyType() throws Exception {
    Mapping mapping = getMapping();
    mapping.m_keyType = Mapping.KeyType.INTEGER;
    Assert.assertEquals( Mapping.KeyType.INTEGER, mapping.getKeyType() );
  }

  @Test
  public void testIsTupleMapping() throws Exception {
    Mapping mapping = getMapping();
    mapping.m_tupleMapping = true;
    Assert.assertTrue( mapping.isTupleMapping() );
  }

  @Test
  public void testSetTupleMapping() throws Exception {
    Mapping mapping = getMapping();
    mapping.setTupleMapping( true );
    Assert.assertTrue( mapping.m_tupleMapping );
  }

  @Test
  public void testGetTupleFamilies() throws Exception {
    Mapping mapping = getMapping();
    mapping.m_tupleFamilies = "test";
    Assert.assertEquals( "test", mapping.getTupleFamilies() );
  }

  @Test
  public void testSetTupleFamilies() throws Exception {
    Mapping mapping = getMapping();
    mapping.setTupleFamilies( "test" );
    Assert.assertEquals( "test", mapping.m_tupleFamilies );
  }

  @Test
  public void testSetMappedColumns() throws Exception {
    Mapping mapping = getMapping();
    mapping.setMappedColumns( new HashMap<String, HBaseValueMeta>() );
    Assert.assertEquals( new HashMap<String, HBaseValueMeta>(), mapping.m_mappedColumnsByAlias );
  }

  @Test
  public void testGetMappedColumns() throws Exception {
    Mapping mapping = getMapping();
    mapping.m_mappedColumnsByAlias = new HashMap<>();
    Assert.assertEquals( new HashMap<String, HBaseValueMeta>(), mapping.getMappedColumns() );
  }

  @Test
  public void testSaveRep() throws Exception {
    Mapping mapping = getMapping();
    Repository rep = Mockito.mock( Repository.class );
    Mockito.doAnswer( new Answer() {

      @Override public Object answer( InvocationOnMock invocation ) throws Throwable {
        Integer i = (Integer) invocation.getArguments()[ 2 ];
        String code = (String) invocation.getArguments()[ 3 ];
        String value = (String) invocation.getArguments()[ 4 ];
        switch ( code ) {
          case "mapping_name": {
            Assert.assertEquals( MAPPING_NAME, value );
            break;
          } case "table_name": {
            Assert.assertEquals( TABLE_NAME, value );
            break;
          } case "key": {
            Assert.assertEquals( KEY, value );
            break;
          } case "key_type": {
            Assert.assertEquals( KEY_TYPE.toString(), value ); break;
          } case "alias": {
            Assert.assertEquals( ALIAS[ i ], value );
            break;
          } case "column_family": {
            Assert.assertEquals( COLUMN_FAMILY[ i ], value );
            break;
          } case "column_name": {
            Assert.assertEquals( COLUMN_NAME[ i ], value );
            break;
          } case "type": {
            Assert.assertEquals( TYPE[ i ], value );
            break;
          } case "indexed_vals": {
            Assert.assertEquals( INDEXED_VALS[ i ], value );
            break;
          }
        }
        return null;
      }
    } ).when( rep )
      .saveStepAttribute( (ObjectId) Matchers.anyObject(), (ObjectId) Matchers.anyObject(), Matchers.anyInt(), Matchers.anyString(), Matchers.anyString() );
    mapping.saveRep( rep, Mockito.mock( ObjectId.class ), Mockito.mock( ObjectId.class ) );
  }

  private String normalForTest( String str ) {
    return str.trim().replace( "\n", "" ).replace( "\r", "" );
  }

  @Test
  public void testGetXML() throws Exception {
    KettleEnvironment.init();
    Mapping mapping = getMapping();
    Assert.assertEquals( normalForTest( XML_NODE ), ( normalForTest( mapping.getXML() ) ) );
  }

  @Test
  public void testLoadXML() throws Exception {
    KettleEnvironment.init();
    Mapping mapping = getMapping();
    Mapping loadMapping = new Mapping();

    Node node = XMLHandler.loadXMLString( XML_NODE );
    Assert.assertTrue( loadMapping.loadXML( node ) );
    Assert.assertEquals( mapping.toString(), loadMapping.toString() );
  }

  @Test
  public void testReadRep() throws Exception {
    KettleEnvironment.init();
    Mapping mapping = getMapping();
    Mapping loadMapping = new Mapping();

    Repository rep = Mockito.mock( Repository.class );
    Mockito.doAnswer( new Answer() {
      @Override public Object answer( InvocationOnMock invocation ) throws Throwable {
        Integer i = (Integer) invocation.getArguments()[ 1 ];
        String code = (String) invocation.getArguments()[ 2 ];
        switch ( code ) {
          case "mapping_name": {
            return MAPPING_NAME;
          } case "table_name": {
            return TABLE_NAME;
          } case "key": {
            return KEY;
          } case "key_type": {
            return KEY_TYPE.toString();
          } case "alias": {
            return ALIAS[ i ];
          } case "column_family": {
            return COLUMN_FAMILY[ i ];
          } case "column_name": {
            return COLUMN_NAME[ i ];
          } case "type": {
            return TYPE[ i ];
          } case "indexed_vals": {
            return INDEXED_VALS[ i ];
          }
        }
        return null;
      }
    } ).when( rep ).getStepAttributeString( (ObjectId) Matchers.anyObject(), Matchers.anyInt(), Matchers.anyString() );
    Mockito.doReturn( true ).when( rep ).getStepAttributeBoolean( (ObjectId) Matchers.anyObject(), Matchers.anyInt(), Matchers.anyString() );
    Mockito.doReturn( 2 ).when( rep ).countNrStepAttributes( (ObjectId) Matchers.anyObject(), Matchers.anyString() );
    Assert.assertTrue( loadMapping.readRep( rep, Mockito.mock( ObjectId.class ) ) );
    Assert.assertEquals( mapping.toString(), loadMapping.toString() );
  }

  @Test
  public void testToString() throws Exception {
    Mapping mapping = getMapping();
    Assert.assertEquals( "Mapping \"mapping_name\" on table \"table_name\":\n"
      + "\n"
      + "\tKEY (key): String\n"
      + "\n"
      + "\t\"alias_2\" (col_family_2,col_name_2): String\n"
      + "\t\"alias_1\" (col_family_1,col_name_1): Number\n", mapping.toString() );
  }

  @Test
  public void testGetXmlWithAllTypes() throws Exception {
    KettleEnvironment.init();
    Mapping mapping = getMappingWithAllTypes();
    Map<String, HBaseValueMeta> columnsMap = mapping.getMappedColumns();
    String xmlMapping = mapping.getXML();
    DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
    Document document = dbFactory.newDocumentBuilder().parse( new ByteArrayInputStream( xmlMapping.getBytes() ) );
    Element rootElement = document.getDocumentElement();
    NodeList nodeList = rootElement.getElementsByTagName( "mapped_column" );
    for ( int i = 0; i < nodeList.getLength(); i++ ) {
      NodeList childNotes = nodeList.item( i ).getChildNodes();
      String rowKeyName = childNotes.item( 1 ).getTextContent();
      String valueType = childNotes.item( 7 ).getTextContent();
      HBaseValueMeta hBaseValueMeta = columnsMap.get( rowKeyName );
      Assert.assertEquals( hBaseValueMeta.getHBaseTypeDesc(), valueType );
    }
  }

}
