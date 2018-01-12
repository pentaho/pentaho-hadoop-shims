/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.hadoop.shim.api.format;

public class AvroSpec {
  public enum DataType {
    NULL(true, "null", null, "Null"),
    BOOLEAN(true, "boolean", null, "Boolean"),
    INTEGER(true, "int", null, "Integer"),
    LONG(true, "long", null, "Long"),
    FLOAT(true, "float", null, "Float"),
    DOUBLE(true, "double", null, "Double"),
    BYTES(true, "bytes", null, "Bytes"),
    STRING(true, "string", null, "String"),
    RECORD(false, "record", null, "Record"),
    ENUM(false, "enum", null, "Enum"),
    ARRAY(false, "array", null, "Array"),
    MAP(false, "map", null, "Map"),
    FIXED(false, "fixed", null, "Fixed"),
    DECIMAL(false, "bytes", "decimal", "Decimal"),
    DATE(false, "int", "date", "Date"),
    TIME_MILLIS(false, "int", "time_millis", "Time"),
    TIME_MICROS(false, "long", "time_micros", "Time In Microseconds"),
    TIMESTAMP_MILLIS(false, "long", "timestamp_millis", "Timestamp"),
    TIMESTAMP_MICROS(false, "long", "timestamp_micros", "Timestamp In Microseconds"),
    DURATION(false, "fixed", "duration", "Duration");

    private final boolean isPrimitive;
    private final String baseType;
    private final String logicalType;
    private final String name;

    DataType(boolean isPrimitiveType, String baseType, String logicalType, String name ) {
      this.isPrimitive = isPrimitiveType;
      this.baseType = baseType;
      this.logicalType = logicalType;
      this.name = name;
    }

    public boolean isPrimitiveType() {
      return isPrimitive;
    }

    public boolean isComplexType() {
      return !isPrimitive && (logicalType == null);
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

    public String getName() {
      return name; }

  }

  public static final String TYPE_RECORD = "record";
  public static final String DOC = "doc";
  public static final String FIELDS_NODE = "fields";
  public static final String NAMESPACE_NODE = "namespace";
  public static final String NAME_NODE = "name";
  public static final String TYPE_NODE = "type";
  public static final String DEFAULT_NODE = "default";
  public static final String LOGICAL_TYPE = "logicalType";
  public static final String DECIMAL_PRECISION = "precision";
  public static final String DECIMAL_SCALE = "scale";
}