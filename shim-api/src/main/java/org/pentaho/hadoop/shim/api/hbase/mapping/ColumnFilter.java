/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/

package org.pentaho.hadoop.shim.api.hbase.mapping;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;

/**
 * Created by bryan on 1/19/16.
 */
public interface ColumnFilter {
  String getFieldAlias();

  void setFieldAlias( String alias );

  String getFieldType();

  void setFieldType( String type );

  ColumnFilter.ComparisonType getComparisonOperator();

  void setComparisonOperator( ColumnFilter.ComparisonType c );

  boolean getSignedComparison();

  void setSignedComparison( boolean signed );

  String getConstant();

  void setConstant( String constant );

  String getFormat();

  void setFormat( String format );

  void appendXML( StringBuilder buff );

  void saveRep( Repository rep, ObjectId id_transformation, ObjectId id_step, int filterNum ) throws
    KettleException;

  enum ComparisonType {
    EQUAL( "=" ),
    GREATER_THAN( ">" ),
    GREATER_THAN_OR_EQUAL( ">=" ),
    LESS_THAN( "<" ),
    LESS_THAN_OR_EQUAL( "<=" ),
    NOT_EQUAL( "!=" ),
    SUBSTRING( "Substring" ),
    REGEX( "Regular expression" ),
    PREFIX( "Starts from" );

    private final String m_stringVal;

    ComparisonType( String name ) {
      this.m_stringVal = name;
    }

    public static String[] getAllOperators() {
      return
        new String[] { ColumnFilter.ComparisonType.EQUAL.toString(), ColumnFilter.ComparisonType.NOT_EQUAL.toString(),
          ColumnFilter.ComparisonType.GREATER_THAN.toString(),
          ColumnFilter.ComparisonType.GREATER_THAN_OR_EQUAL.toString(),
          ColumnFilter.ComparisonType.LESS_THAN.toString(), ColumnFilter.ComparisonType.LESS_THAN_OR_EQUAL.toString(),
          ColumnFilter.ComparisonType.SUBSTRING.toString(), ColumnFilter.ComparisonType.PREFIX.toString(),
          ColumnFilter.ComparisonType.REGEX.toString() };
    }

    public static String[] getStringOperators() {
      return
        new String[] { ColumnFilter.ComparisonType.SUBSTRING.toString(), ColumnFilter.ComparisonType.PREFIX.toString(),
          ColumnFilter.ComparisonType.REGEX.toString() };
    }

    public static String[] getNumericOperators() {
      return
        new String[] { ColumnFilter.ComparisonType.EQUAL.toString(), ColumnFilter.ComparisonType.NOT_EQUAL.toString(),
          ColumnFilter.ComparisonType.GREATER_THAN.toString(),
          ColumnFilter.ComparisonType.GREATER_THAN_OR_EQUAL.toString(),
          ColumnFilter.ComparisonType.LESS_THAN.toString(), ColumnFilter.ComparisonType.LESS_THAN_OR_EQUAL.toString() };
    }

    public static ColumnFilter.ComparisonType stringToOpp( String opp ) {
      for ( ComparisonType comparisonType : ComparisonType.values() ) {
        if ( comparisonType.toString().equals( opp ) ) {
          return comparisonType;
        }
      }
      return null;
    }

    public String toString() {
      return this.m_stringVal;
    }
  }
}
