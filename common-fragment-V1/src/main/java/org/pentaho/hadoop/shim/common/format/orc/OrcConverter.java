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
import org.apache.logging.log4j.LogManager;
import org.apache.orc.TypeDescription;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaBase;
import org.pentaho.di.core.row.value.ValueMetaConversionException;
import org.pentaho.di.core.row.value.ValueMetaConverter;
import org.pentaho.hadoop.shim.api.format.IOrcInputField;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.Logger;

/**
 * Created by tkafalas 11/7/2017
 */
public class OrcConverter {
  private ValueMetaConverter valueMetaConverter = new ValueMetaConverter();
  private static final Logger logger = LogManager.getLogger( OrcConverter.class );

  public RowMetaAndData convertFromOrc( VectorizedRowBatch batch, int currentBatchRow,
                                        List<? extends IOrcInputField> dialogInputFields,
                                        TypeDescription typeDescription,
                                        Map<String, Integer> schemaToOrcSubcripts,
                                        List<? extends IOrcInputField> orcInputFields ) {
    return convertFromOrc( new RowMetaAndData(), batch, currentBatchRow, dialogInputFields, typeDescription,
      schemaToOrcSubcripts, orcInputFields );
  }

  @VisibleForTesting
  RowMetaAndData convertFromOrc( RowMetaAndData rowMetaAndData, VectorizedRowBatch batch, int currentBatchRow,
                                 List<? extends IOrcInputField> dialogInputFields, TypeDescription typeDescription,
                                 Map<String, Integer> schemaToOrcSubcripts,
                                 List<? extends IOrcInputField> orcInputFields ) {

    for ( IOrcInputField inputField : dialogInputFields ) {
      IOrcInputField orcField = getFormatField( inputField.getFormatFieldName(), orcInputFields );
      if ( inputField != null ) {
        ColumnVector columnVector = batch.cols[ schemaToOrcSubcripts.get( inputField.getPentahoFieldName() ) ];
        Object orcToPentahoValue =
          convertFromSourceToTargetDataType( columnVector, currentBatchRow, orcField.getPentahoType() );

        Object convertToSchemaValue = null;
        try {
          String dateFormatStr = inputField.getStringFormat();
          if ( ( dateFormatStr == null ) || ( dateFormatStr.trim().length() == 0 ) ) {
            dateFormatStr = ValueMetaBase.DEFAULT_DATE_FORMAT_MASK;
          }
          valueMetaConverter.setDatePattern( new SimpleDateFormat( dateFormatStr ) );
          convertToSchemaValue = valueMetaConverter
            .convertFromSourceToTargetDataType( orcField.getPentahoType(), inputField.getPentahoType(),
              orcToPentahoValue );
        } catch ( ValueMetaConversionException e ) {
          logger.error( e );
        }
        rowMetaAndData.addValue( inputField.getPentahoFieldName(), inputField.getPentahoType(), convertToSchemaValue );
        String stringFormat = inputField.getStringFormat();
        if ( ( stringFormat != null ) && ( stringFormat.trim().length() > 0 ) ) {
          rowMetaAndData.getValueMeta( rowMetaAndData.size() - 1 ).setConversionMask( stringFormat );
        }
      }
    }

    return rowMetaAndData;
  }

  protected static Object convertFromSourceToTargetDataType( ColumnVector columnVector, int currentBatchRow,
                                                             int orcValueMetaInterface ) {

    int indexToLookup = columnVector.isRepeating ? 0 : currentBatchRow;
    if ( columnVector.isNull[ indexToLookup ] ) {
      return null;
    }
    switch ( orcValueMetaInterface ) {
      case ValueMetaInterface.TYPE_INET:
        try {
          return InetAddress.getByName( new String( ( (BytesColumnVector) columnVector ).vector[ indexToLookup ],
            ( (BytesColumnVector) columnVector ).start[ indexToLookup ],
            ( (BytesColumnVector) columnVector ).length[ indexToLookup ] ) );
        } catch ( UnknownHostException e ) {
          e.printStackTrace();
        }
        break;

      case ValueMetaInterface.TYPE_STRING:
        return new String( ( (BytesColumnVector) columnVector ).vector[ indexToLookup ],
          ( (BytesColumnVector) columnVector ).start[ indexToLookup ],
          ( (BytesColumnVector) columnVector ).length[ indexToLookup ] );

      case ValueMetaInterface.TYPE_INTEGER:
        return ( (LongColumnVector) columnVector ).vector[ indexToLookup ];

      case ValueMetaInterface.TYPE_NUMBER:
        return ( (DoubleColumnVector) columnVector ).vector[ indexToLookup ];

      case ValueMetaInterface.TYPE_BIGNUMBER:
        HiveDecimalWritable obj = ( (DecimalColumnVector) columnVector ).vector[ indexToLookup ];
        return obj.getHiveDecimal().bigDecimalValue();

      case ValueMetaInterface.TYPE_TIMESTAMP:
        Timestamp timestamp = new Timestamp( ( (TimestampColumnVector) columnVector ).time[ indexToLookup ] );
        timestamp.setNanos( ( (TimestampColumnVector) columnVector ).nanos[ indexToLookup ] );
        return timestamp;

      case ValueMetaInterface.TYPE_DATE:
        LocalDate localDate =
          LocalDate.ofEpochDay( 0 ).plusDays( ( (LongColumnVector) columnVector ).vector[ indexToLookup ] );
        Date dateValue = Date.from( localDate.atStartOfDay( ZoneId.systemDefault() ).toInstant() );
        return dateValue;

      case ValueMetaInterface.TYPE_BOOLEAN:
        return ( (LongColumnVector) columnVector ).vector[ indexToLookup ] != 0;

      case ValueMetaInterface.TYPE_BINARY:
        byte[] origBytes = ( (BytesColumnVector) columnVector ).vector[ indexToLookup ];
        int startPos = ( (BytesColumnVector) columnVector ).start[ indexToLookup ];
        byte[] newBytes = Arrays.copyOfRange( origBytes, startPos,
          startPos + ( (BytesColumnVector) columnVector ).length[ indexToLookup ] );
        return newBytes;
      default:
        return null;
    }

    //if none of the cases match return a null
    return null;
  }

  public IOrcInputField getFormatField( String formatFieldName, List<? extends IOrcInputField> fields ) {
    if ( formatFieldName == null || formatFieldName.trim().isEmpty() ) {
      return null;
    }

    for ( IOrcInputField field : fields ) {
      if ( field.getFormatFieldName().equals( formatFieldName ) ) {
        return field;
      }
    }

    return null;
  }

}
