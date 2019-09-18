Parquet files support requires data types conversion because we have several data type sets depends on system:
1. Kettle's core data types (see ValueMetaInterface)
2. Parquet's format data types (see Parquet's primitive types) with logical datatypes marks (see Parquet's LogicalTypes.md)
3. Spark's sql data types (see org.apache.spark.sql.types.DataTypes)

As result, we can have different conversion for the same datatype depends on way for save file: Spark writes date/timestamp to INT96, but we can write it to INT64. Other system can have different assumptions about datatype conversion. This file describes our assumptions for datatype conversion. Other types(including custom) are not supported.


Kettle's Type | Kettle's Java Class  | Parquet primitive      | Spark -> Parquet
--------------|----------------------|------------------------|---------------------------
NUMBER        | java.lang.Double     | DOUBLE                 | DoubleType -> DOUBLE
STRING        | java.lang.String     | BINARY+UTF8 (1)        | StringType -> BINARY+UTF8
DATE          | java.util.Date       | INT64+TIMESTAMP_MILLIS | TimestampType -> INT96 (2)
BOOLEAN       | java.lang.Boolean    | BOOLEAN                | BooleanType -> BOOLEAN
INTEGER       | java.lang.Long       | INT64+INT_64           | LongType -> INT64
BIGNUMBER     | java.math.BigDecimal | DOUBLE                 | DoubleType -> DOUBLE (3)
BINARY        | byte[]               | BINARY                 | BinaryType -> BINARY
TIMESTAMP     | java.sql.Timestamp   | INT64+TIMESTAMP_MILLIS | TimestampType -> INT96 (2)

1) Primitive type + Logical type
2) We can't convert it to DateType because DateType contains only 4 bytes for day representation only, but Kettle's date contains up to milliseconds.
3) We can't convert to DecimalType because it requires declared scale>=0, but Kettle type declaration can not have such info.

"Get Fields" button tries to retrieve schema from Parquet file and create data types using assumptions:

Parquet type | Kettle's type
-------------|--------------
BINARY       | STRING
BOOLEAN      | BOOLEAN
DOUBLE       | NUMBER
FLOAT        | NUMBER
INT32        | INTEGER or DATE (4)
INT64        | INTEGER or DATE (4)
INT96        | DATE

4) DATE in case of logical type is one of: DATE, TIME_MILLIS, TIMESTAMP_MILLIS

Data type in Parquet Input can be changed for process correct Kettle's type.
