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

package org.pentaho.hadoop.shim.api.format;

import org.pentaho.di.core.row.ValueMetaInterface;

import java.util.Arrays;

import static java.util.Arrays.asList;

public class ParquetSpec {
  @SuppressWarnings( "squid:S1192" ) // string constants
  public enum DataType {
    NULL( 0, true, "null", null, false, "Null", ValueMetaInterface.TYPE_NONE ),
    BINARY( 1, true, "binary", null, true, "ByteArray", ValueMetaInterface.TYPE_BINARY ),
    BOOLEAN( 2, true, "boolean", null, true, "Boolean", ValueMetaInterface.TYPE_BOOLEAN ),
    DOUBLE( 3, true, "double", null, true, "Double", ValueMetaInterface.TYPE_NUMBER ),
    FLOAT( 4, true, "float", null, true, "Float", ValueMetaInterface.TYPE_NUMBER ),
    INT_32( 5, true, "int32", null, true, "Int32", ValueMetaInterface.TYPE_INTEGER ),
    FIXED_LEN_BYTE_ARRAY( 6, true, "fixed_len_byte_array", null, true, "FixedLengthByteArray",
      ValueMetaInterface.TYPE_BINARY ),
    DECIMAL( 7, false, "binary", null, true, "Decimal", ValueMetaInterface.TYPE_BIGNUMBER ),
    DECIMAL_INT_32( 8, false, "int32", null, true, "Decimal", ValueMetaInterface.TYPE_BIGNUMBER ),
    DECIMAL_INT_64( 9, false, "int64", null, true, "Decimal", ValueMetaInterface.TYPE_BIGNUMBER ),
    DECIMAL_FIXED_LEN_BYTE_ARRAY( 10, false, "fixed_len_byte_array", null, true, "Decimal",
      ValueMetaInterface.TYPE_BIGNUMBER ),
    DATE( 11, false, "int32", null, true, "Date", ValueMetaInterface.TYPE_DATE ),
    ENUM( 12, false, "binary", null, true, "Enum", ValueMetaInterface.TYPE_STRING ),
    INT_8( 13, false, "int32", null, true, "Int8", ValueMetaInterface.TYPE_INTEGER ),
    INT_16( 14, false, "int32", null, true, "Int16", ValueMetaInterface.TYPE_INTEGER ),
    INT_64( 15, false, "int64", null, true, "Int64", ValueMetaInterface.TYPE_INTEGER ),
    INT_96( 16, false, "int96", null, true, "Int96", ValueMetaInterface.TYPE_TIMESTAMP ),
    UINT_8( 17, false, "int32", null, true, "UInt8", ValueMetaInterface.TYPE_INTEGER ),
    UINT_16( 18, false, "int32", null, true, "UInt16", ValueMetaInterface.TYPE_INTEGER ),
    UINT_32( 19, false, "int32", null, true, "UInt32", ValueMetaInterface.TYPE_INTEGER ),
    UINT_64( 20, false, "int64", null, true, "UInt64", ValueMetaInterface.TYPE_INTEGER ),
    UTF8( 21, false, "binary", null, true, "UTF8", ValueMetaInterface.TYPE_STRING ),
    TIME_MILLIS( 22, false, "int32", null, true, "TimeMillis", ValueMetaInterface.TYPE_TIMESTAMP ),
    TIMESTAMP_MILLIS( 23, false, "int64", null, true, "TimestampMillis", ValueMetaInterface.TYPE_TIMESTAMP ),
    BSON( 24, false, "binary", null, false, "BSON", ValueMetaInterface.TYPE_NONE ),
    INTERVAL( 25, false, "fixed_len_byte_array", null, false, "Interval", ValueMetaInterface.TYPE_NONE ),
    JSON( 26, false, "binary", null, false, "JSON", ValueMetaInterface.TYPE_NONE ),
    LIST( 27, false, "", null, false, "List", ValueMetaInterface.TYPE_NONE ),
    MAP( 28, false, "", null, false, "Map", ValueMetaInterface.TYPE_NONE ),
    MAP_KEY_VALUE( 29, false, "", null, false, "MapKeyValue", ValueMetaInterface.TYPE_NONE ),
    STRUCT( 30, false, "", null, false, "Struct", ValueMetaInterface.TYPE_NONE ),
    UNION( 31, false, "", null, false, "Union", ValueMetaInterface.TYPE_NONE );

    private final int id;
    private final boolean isPrimitive;
    private final String baseType;
    private final String logicalType;
    private final boolean displayable;
    private final String name;
    private final int pdiType;

    DataType( int id, boolean isPrimitiveType, String baseType, String logicalType, boolean displayable, String name,
              int pdiType ) {
      this.id = id;
      this.isPrimitive = isPrimitiveType;
      this.baseType = baseType;
      this.logicalType = logicalType;
      this.displayable = displayable;
      this.name = name;
      this.pdiType = pdiType;
    }

    public static ParquetSpec.DataType getDataType( int id ) {
      // enum .values() returns vals in order they are defined
      return asList( DataType.values() ).get( id );
    }

    public int getId() {
      return this.id;
    }

    public boolean isPrimitiveType() {
      return isPrimitive;
    }

    public boolean isComplexType() {
      return !isPrimitive && ( logicalType == null );
    }

    public boolean isLogicalType() {
      return logicalType != null;
    }

    public String getBaseType() {
      return baseType;
    }

    public String getLogicalType() {
      return logicalType;
    }

    public String getType() {
      return isLogicalType() ? logicalType : baseType;
    }

    public boolean isDisplayable() {
      return this.displayable;
    }

    public String getName() {
      return name;
    }

    public int getPdiType() {
      return pdiType;
    }

    public static String[] getDisplayableTypeNames() {
      return Arrays.stream( ParquetSpec.DataType.values() )
        .filter( ParquetSpec.DataType::isDisplayable )
        .map( ParquetSpec.DataType::getName )
        .sorted()
        .toArray( String[]::new );
    }
  }

  public static final int DEFAULT_DECIMAL_PRECISION = 20;
  public static final int DEFAULT_DECIMAL_SCALE = 0;
  public static final int JULIAN_DAY_OF_EPOCH = 2440588;
}
