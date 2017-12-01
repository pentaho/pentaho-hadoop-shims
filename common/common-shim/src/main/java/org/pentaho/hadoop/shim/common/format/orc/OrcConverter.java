/*! ******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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
package org.pentaho.hadoop.shim.common.format.orc;


import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.orc.TypeDescription;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.hadoop.shim.api.format.SchemaDescription;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;

/**
 * Created by tkafalaf 11/7/2017
 */
public class OrcConverter {

  public static RowMetaAndData convertFromOrc( VectorizedRowBatch batch, int currentBatchRow,
                                               SchemaDescription schemaDescription, TypeDescription typeDescription,
                                               Map<String, Integer> schemaToOrcSubcripts, SchemaDescription orcSchemaDescription ) {
    return convertFromOrc( new RowMetaAndData(), batch, currentBatchRow, schemaDescription, typeDescription,
      schemaToOrcSubcripts, orcSchemaDescription );
  }

  @VisibleForTesting
  static RowMetaAndData convertFromOrc( RowMetaAndData rowMetaAndData, VectorizedRowBatch batch, int currentBatchRow,
                                        SchemaDescription schemaDescription, TypeDescription typeDescription,
                                        Map<String, Integer> schemaToOrcSubcripts,
                                        SchemaDescription orcSchemaDescription ) {

    int orcColumn;
    for ( SchemaDescription.Field field : schemaDescription ) {
      SchemaDescription.Field orcField = orcSchemaDescription.getField( field.formatFieldName );
      if ( field != null ) {
        ColumnVector columnVector = batch.cols[ schemaToOrcSubcripts.get( field.pentahoFieldName ) ];
        Object data = convertFromSourceToTargetDataType( columnVector, currentBatchRow, field.pentahoValueMetaType );
        rowMetaAndData.addValue( field.pentahoFieldName, field.pentahoValueMetaType, data );
      }
    }

    return rowMetaAndData;
  }

  protected static Object convertFromSourceToTargetDataType( ColumnVector columnVector, int currentBatchRow,
                                                             int valueMetaInterface ) {

    if ( columnVector.isNull[currentBatchRow] ) {
      return null;
    }
    switch ( valueMetaInterface ) {
      case ValueMetaInterface.TYPE_INET:
        try {
          return InetAddress.getByName( new String( ( (BytesColumnVector) columnVector ).vector[ currentBatchRow ],
            ( (BytesColumnVector) columnVector ).start[ currentBatchRow ],
            ( (BytesColumnVector) columnVector ).length[ currentBatchRow ] ) );
        } catch ( UnknownHostException e ) {
          e.printStackTrace();
        }

      case ValueMetaInterface.TYPE_STRING:
        return new String( ( (BytesColumnVector) columnVector ).vector[ currentBatchRow ],
          ( (BytesColumnVector) columnVector ).start[ currentBatchRow ],
          ( (BytesColumnVector) columnVector ).length[ currentBatchRow ] );

      case ValueMetaInterface.TYPE_INTEGER:
        return (long) ( (LongColumnVector) columnVector ).vector[ currentBatchRow ];

      case ValueMetaInterface.TYPE_NUMBER:
        return ( (DoubleColumnVector) columnVector ).vector[ currentBatchRow ];

      case ValueMetaInterface.TYPE_BIGNUMBER:
        HiveDecimalWritable obj = ( (DecimalColumnVector) columnVector ).vector[ currentBatchRow ];
        return obj.getHiveDecimal().bigDecimalValue();

      case ValueMetaInterface.TYPE_TIMESTAMP:
        Timestamp timestamp = new Timestamp( ( (TimestampColumnVector) columnVector ).time[ currentBatchRow ] );
        timestamp.setNanos( ( (TimestampColumnVector) columnVector ).nanos[ currentBatchRow ] );
        return timestamp;

      case ValueMetaInterface.TYPE_DATE:
        return new Date( ( (LongColumnVector) columnVector ).vector[ currentBatchRow ] );

      case ValueMetaInterface.TYPE_BOOLEAN:
        return ( (LongColumnVector) columnVector ).vector[ currentBatchRow ] == 0 ? false : true;

      case ValueMetaInterface.TYPE_BINARY:
        byte[] origBytes = ( (BytesColumnVector) columnVector ).vector[ currentBatchRow ];
        int startPos = ( (BytesColumnVector) columnVector ).start[ currentBatchRow ];
        byte[] newBytes = Arrays.copyOfRange( origBytes, startPos,
          startPos + ( (BytesColumnVector) columnVector ).length[ currentBatchRow ] );
        return newBytes;
    }

    //if none of the cases match return a null
    return null;
  }

}

