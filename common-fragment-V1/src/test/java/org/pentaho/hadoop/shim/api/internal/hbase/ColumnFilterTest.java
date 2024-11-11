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


package org.pentaho.hadoop.shim.api.internal.hbase;

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.repository.ObjectId;
import org.w3c.dom.Node;
import org.pentaho.di.repository.Repository;


import static org.junit.Assert.*;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

/**
 * User: Dzmitry Stsiapanau Date: 10/16/2015 Time: 08:38
 */

public class ColumnFilterTest extends ColumnFilter {

  public static final String XML_NODE = "<filter>\n"
    + "            <alias>field_alias</alias>\n"
    + "\n"
    + "            <type>type</type>\n"
    + "\n"
    + "            <comparison_opp>=</comparison_opp>\n"
    + "\n"
    + "            <signed_comp>Y</signed_comp>\n"
    + "\n"
    + "            <constant>constant</constant>\n"
    + "\n"
    + "            <format>format</format>\n"
    + "\n"
    + "        </filter>";
  public static final String FIELD_ALIAS = "field_alias";
  public static final String CONSTANT = "constant";
  public static final String FORMAT = "format";
  public static final String TYPE = "type";
  public static final ComparisonType OPERATOR = ComparisonType.EQUAL;
  public static final Boolean SIGNED_COMPARISON = true;

  public ColumnFilterTest() {
    super( "" );
  }

  @Test
  public void testSetFieldAlias() throws Exception {
    ColumnFilter cf = new ColumnFilterTest();
    cf.setFieldAlias( "test" );
    assertEquals( "test", cf.m_fieldAlias );
  }

  @Test
  public void testGetFieldAlias() throws Exception {
    ColumnFilter cf = new ColumnFilterTest();
    cf.m_fieldAlias = "test";
    assertEquals( "test", cf.getFieldAlias() );
  }

  @Test
  public void testSetFieldType() throws Exception {
    ColumnFilter cf = new ColumnFilterTest();
    cf.setFieldType( "test" );
    assertEquals( "test", cf.m_fieldType );
  }

  @Test
  public void testGetFieldType() throws Exception {
    ColumnFilter cf = new ColumnFilterTest();
    cf.m_fieldType = "test";
    assertEquals( "test", cf.getFieldType() );
  }

  @Test
  public void testSetComparisonOperator() throws Exception {
    ColumnFilter cf = new ColumnFilterTest();
    cf.setComparisonOperator( ComparisonType.EQUAL );
    assertEquals( ComparisonType.EQUAL, cf.m_comparison );
  }

  @Test
  public void testGetComparisonOperator() throws Exception {
    ColumnFilter cf = new ColumnFilterTest();
    cf.m_comparison = ComparisonType.EQUAL;
    assertEquals( ComparisonType.EQUAL, cf.getComparisonOperator() );
  }

  @Test
  public void testSetSignedComparison() throws Exception {
    ColumnFilter cf = new ColumnFilterTest();
    cf.setSignedComparison( true );
    assertTrue( cf.m_signedComparison );
  }

  @Test
  public void testGetSignedComparison() throws Exception {
    ColumnFilter cf = new ColumnFilterTest();
    cf.m_signedComparison = true;
    assertTrue( cf.getSignedComparison() );
  }

  @Test
  public void testSetConstant() throws Exception {
    ColumnFilter cf = new ColumnFilterTest();
    cf.setConstant( "test" );
    assertEquals( "test", cf.m_constant );
  }

  @Test
  public void testGetConstant() throws Exception {
    ColumnFilter cf = new ColumnFilterTest();
    cf.m_constant = "test";
    assertEquals( "test", cf.getConstant() );
  }

  @Test
  public void testSetFormat() throws Exception {
    ColumnFilter cf = new ColumnFilterTest();
    cf.setFormat( "test" );
    assertEquals( "test", cf.m_format );
  }

  @Test
  public void testGetFormat() throws Exception {
    ColumnFilter cf = new ColumnFilterTest();
    cf.m_format = "test";
    assertEquals( "test", cf.getFormat() );
  }

  @Test
  public void testAppendXML() throws Exception {
    ColumnFilter cf = getCF();
    StringBuffer buff = new StringBuffer();
    cf.appendXML( buff );
    assertEquals( normalForTest( XML_NODE ), ( normalForTest( buff.toString() ) ) );
  }

  private String normalForTest( String str ) {
    return str.trim().replace( "\n", "" ).replace( "\r", "" );
  }

  @Test
  public void testGetFilter() throws Exception {
    ColumnFilter cf = getCF();
    Node node = XMLHandler.loadXMLString( XML_NODE, "filter" );
    assertTrue( equalsCF( cf, getFilter( node ) ) );
  }

  private ColumnFilter getCF() {
    ColumnFilter cf = new ColumnFilter( "" );
    cf.setFieldAlias( FIELD_ALIAS );
    cf.setConstant( CONSTANT );
    cf.setFormat( FORMAT );
    cf.setSignedComparison( SIGNED_COMPARISON );
    cf.setComparisonOperator( OPERATOR );
    cf.setFieldType( TYPE );
    return cf;
  }

  @Test
  public void testSaveRep() throws Exception {
    ColumnFilter cf = getCF();
    Repository rep = mock( Repository.class );
    doAnswer( new Answer() {

      @Override public Object answer( InvocationOnMock invocation ) throws Throwable {
        String code = (String) invocation.getArguments()[ 3 ];
        String value = (String) invocation.getArguments()[ 4 ];
        switch ( code ) {
          case "cf_alias": {
            assertEquals( FIELD_ALIAS, value );
            break;
          }
          case "cf_type": {
            assertEquals( TYPE, value );
            break;
          }
          case "cf_comparison_opp": {
            assertEquals( OPERATOR.toString(), value );
            break;
          }
          case "cf_signed_comp": {
            assertEquals( SIGNED_COMPARISON ? "Y" : "N", value );
            break;
          }
          case "cf_constant": {
            assertEquals( CONSTANT, value );
            break;
          }
          case "cf_format": {
            assertEquals( FORMAT, value );
            break;
          }
        }
        return null;
      }
    } ).when( rep )
      .saveStepAttribute( (ObjectId) anyObject(), (ObjectId) anyObject(), anyInt(), anyString(), anyString() );
    cf.saveRep( rep, mock( ObjectId.class ), mock( ObjectId.class ), 0 );
  }

  @Test
  public void testGetFilterFromRepo() throws Exception {
    ColumnFilter cf = getCF();
    Repository rep = mock( Repository.class );
    doAnswer( new Answer() {
      @Override public Object answer( InvocationOnMock invocation ) throws Throwable {
        String code = (String) invocation.getArguments()[ 2 ];
        switch ( code ) {
          case "cf_alias": {
            return FIELD_ALIAS;
          }
          case "cf_type": {
            return TYPE;
          }
          case "cf_comparison_opp": {
            return OPERATOR.toString();
          }
          case "cf_signed_comp": {
            return SIGNED_COMPARISON ? "Y" : "N";
          }
          case "cf_constant": {
            return CONSTANT;
          }
          case "cf_format": {
            return FORMAT;
          }
        }
        return null;
      }
    } ).when( rep ).getStepAttributeString( (ObjectId) anyObject(), anyInt(), anyString() );
    doReturn( true ).when( rep ).getStepAttributeBoolean( (ObjectId) anyObject(), anyInt(), anyString() );
    assertTrue( equalsCF( cf, getFilter( rep, 0, mock( ObjectId.class ) ) ) );
  }

  @Test
  public void testStringToOpp() throws Exception {
    assertEquals( ComparisonType.EQUAL, stringToOpp( "=" ) );
    assertEquals( ComparisonType.GREATER_THAN, stringToOpp( ">" ) );
    assertEquals( ComparisonType.GREATER_THAN_OR_EQUAL, stringToOpp( ">=" ) );
    assertEquals( ComparisonType.LESS_THAN, stringToOpp( "<" ) );
    assertEquals( ComparisonType.LESS_THAN_OR_EQUAL, stringToOpp( "<=" ) );
    assertEquals( ComparisonType.NOT_EQUAL, stringToOpp( "!=" ) );
    assertEquals( ComparisonType.PREFIX, stringToOpp( "Starts from" ) );
    assertEquals( ComparisonType.REGEX, stringToOpp( "Regular expression" ) );
    assertEquals( ComparisonType.SUBSTRING, stringToOpp( "Substring" ) );
  }

  @Test
  public void testGetAllOperators() throws Exception {
    assertEquals( 9, getAllOperators().length );
  }

  @Test
  public void testGetStringOperators() throws Exception {
    assertEquals( 3, getStringOperators().length );

  }

  @Test
  public void testGetNumericOperators() throws Exception {
    assertEquals( 6, getNumericOperators().length );
  }

  public boolean equalsCF( ColumnFilter one, ColumnFilter another ) {
    if ( one == another ) {
      return true;
    }
    if ( another == null || one == null ) {
      return false;
    }

    if ( one.m_signedComparison != another.m_signedComparison ) {
      return false;
    }
    if ( one.m_comparison != another.m_comparison ) {
      return false;
    }
    if ( !one.m_constant.equals( another.m_constant ) ) {
      return false;
    }
    if ( !one.m_fieldAlias.equals( another.m_fieldAlias ) ) {
      return false;
    }
    if ( one.m_fieldType != null ? !one.m_fieldType.equals( another.m_fieldType ) : another.m_fieldType != null ) {
      return false;
    }
    if ( one.m_format != null ? !one.m_format.equals( another.m_format ) : another.m_format != null ) {
      return false;
    }

    return true;
  }
}
