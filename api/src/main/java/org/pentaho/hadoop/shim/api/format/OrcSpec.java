package org.pentaho.hadoop.shim.api.format;

import org.pentaho.di.core.row.ValueMetaInterface;

import java.util.Arrays;
import java.util.ArrayList;

public class OrcSpec {
  public enum DataType {
    NULL( 0, true, "null", null, false, "Null", ValueMetaInterface.TYPE_NONE ),
    BOOLEAN( 1, true, "LongColumnVector", null, true, "Boolean", ValueMetaInterface.TYPE_BOOLEAN ),
    TINYINT( 2, true, "LongColumnVector", null, true, "TinyInt", ValueMetaInterface.TYPE_INTEGER ),
    SMALLINT( 3, true, "LongColumnVector", null, true, "SmallInt", ValueMetaInterface.TYPE_INTEGER ),
    INTEGER( 4, true, "LongColumnVector", null, true, "Int", ValueMetaInterface.TYPE_INTEGER ),
    BIGINT( 5, true, "LongColumnVector", null, true, "BigInt", ValueMetaInterface.TYPE_INTEGER ),
    BINARY( 6, true, "BytesColumnVector", null, true, "Binary", ValueMetaInterface.TYPE_BINARY ),
    FLOAT( 7, true, "DoubleColumnVector", null, true, "Float", ValueMetaInterface.TYPE_NUMBER ),
    DOUBLE( 8, true, "DoubleColumnVector", null, true, "Double", ValueMetaInterface.TYPE_NUMBER ),
    DECIMAL( 9, true, "DecimalColumnVector", null, true, "Decimal", ValueMetaInterface.TYPE_BIGNUMBER ),
    STRING( 10, true, "BytesColumnVector", null, true, "String", ValueMetaInterface.TYPE_STRING ),
    CHAR( 11, true, "BytesColumnVector", null, true, "Char", ValueMetaInterface.TYPE_STRING ),
    VARCHAR( 12, true, "BytesColumnVector", null, true, "VarChar", ValueMetaInterface.TYPE_STRING ),
    TIMESTAMP( 13, true, "TimestampColumnVector", null, true, "Timestamp", ValueMetaInterface.TYPE_TIMESTAMP ),
    DATE( 14, true, "LongColumnVector", null, true, "Date", ValueMetaInterface.TYPE_DATE ),
    STRUCT( 15, true, "StructColumnVector", null, false, "Struct", ValueMetaInterface.TYPE_NONE ),
    LIST( 16, true, "ListColumnVector", null, false, "List", ValueMetaInterface.TYPE_NONE ),
    MAP( 17, true, "MapColumnVector", null, false, "List", ValueMetaInterface.TYPE_NONE ),
    UNION( 18, true, "UnionColumnVector", null, false, "Union", ValueMetaInterface.TYPE_NONE );

    private final int id;
    private final boolean isPrimitive;
    private final String baseType;
    private final String logicalType;
    private final boolean displayable;
    private final String name;
    private final int pentahoType;
    private static final ArrayList<DataType> enumValues = new ArrayList<DataType>() {
      {
        for ( DataType dataType : DataType.values() ) {
          add( dataType.getId(), dataType );
        }
      }
    };

    DataType( int id, boolean isPrimitiveType, String baseType, String logicalType, boolean displayable, String name, int pentahoType ) {
      this.id = id;
      this.isPrimitive = isPrimitiveType;
      this.baseType = baseType;
      this.logicalType = logicalType;
      this.displayable = displayable;
      this.name = name;
      this.pentahoType = pentahoType;
    }

    public static DataType getDataType( int id ) {
      return enumValues.get( id );
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

    public int getPentahoType() {
      return pentahoType;
    }

    public static String[] getDisplayableTypeNames() {
      return Arrays.stream( OrcSpec.DataType.values() )
        .filter( DataType::isDisplayable )
        .map( DataType::getName )
        .sorted()
        .toArray( String[]::new );
    }
  }

  public static final int DEFAULT_DECIMAL_PRECISION = 20;
  public static final int DEFAULT_DECIMAL_SCALE = 10;
}
