/*! ******************************************************************************
 *
 * Pentaho Big Data
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
package org.pentaho.hadoop.shim.common.format.avro;

import org.apache.avro.generic.GenericRecord;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.hadoop.shim.api.format.SchemaDescription;

import java.math.BigDecimal;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith( Parameterized.class )
public class AvroConverterFromAvroTest {

  private String formatFieldName;
  private String pentahoFieldName;
  private int metaType;
  private int avroType;
  private Object value;
  private Object expected;

  public AvroConverterFromAvroTest(String formatFieldName, String pentahoFieldName, int avroType, int metaType, Object value,
                                   Object expected ) {
    this.formatFieldName = formatFieldName;
    this.pentahoFieldName = pentahoFieldName;
    this.metaType = metaType;
    this.avroType = avroType;
    this.value = value;
    this.expected = expected;
  }

  @Parameterized.Parameters
  public static Collection values() {
    return Arrays.asList( new Object[][] {
      { "inetFormatField", "inetPentahoField", ValueMetaInterface.TYPE_INET, ValueMetaInterface.TYPE_INET, "127.0.0.1", InetAddress.getLoopbackAddress() },
      { "stringFormatField", "stringPentahoField", ValueMetaInterface.TYPE_STRING, ValueMetaInterface.TYPE_STRING, "stringData", "stringData" },
      { "integerFormatField", "integerPentahoField", ValueMetaInterface.TYPE_INTEGER, ValueMetaInterface.TYPE_INTEGER, 5L, 5L },
      { "numberFormatField", "numberPentahoField", ValueMetaInterface.TYPE_NUMBER, ValueMetaInterface.TYPE_NUMBER, 7d, 7.0 },
      { "bigNumberFormatField", "bigNumberPentahoField", ValueMetaInterface.TYPE_BIGNUMBER, ValueMetaInterface.TYPE_BIGNUMBER, 77d, new BigDecimal( 77d ) },
      { "tsFormatField", "tsPentahoField", ValueMetaInterface.TYPE_TIMESTAMP, ValueMetaInterface.TYPE_TIMESTAMP, 10L, new Timestamp( 10L ) },
      { "dateFormatField", "datePentahoField", ValueMetaInterface.TYPE_DATE, ValueMetaInterface.TYPE_DATE,
        1, Date.from( LocalDate.ofEpochDay( 1 ).atStartOfDay( ZoneId.systemDefault() ).toInstant() ) },
      { "booleanFormatField", "booleanPentahoField", ValueMetaInterface.TYPE_BOOLEAN, ValueMetaInterface.TYPE_BOOLEAN, true, true },
      { "binaryFormatField", "binaryPentahoField", ValueMetaInterface.TYPE_BINARY, ValueMetaInterface.TYPE_BINARY,
        ByteBuffer.wrap( new byte[] {1, 2, 3} ), new byte[] {1, 2, 3} },
      { "stringToNumberFormatField", "stringToNumberPentahoField", ValueMetaInterface.TYPE_STRING, ValueMetaInterface.TYPE_NUMBER, "2344234", 2344234.0 },
      { "booleanToStringFormatField", "sbooleanToStringPentahoField", ValueMetaInterface.TYPE_BOOLEAN, ValueMetaInterface.TYPE_STRING, true, "true" },
      { "booleanToIntegerFormatField", "sbooleanToIntegerPentahoField", ValueMetaInterface.TYPE_BOOLEAN, ValueMetaInterface.TYPE_INTEGER, true, null }
    } );
  }


  @Test
  public void convertFromAvro() throws Exception {
    GenericRecord record = mock( GenericRecord.class );
    when( record.get( formatFieldName ) ).thenReturn( value );
    SchemaDescription metaSchemaDescription = new SchemaDescription();
    SchemaDescription avroSchemaDescription = new SchemaDescription();
    metaSchemaDescription.addField( metaSchemaDescription.new Field( formatFieldName, pentahoFieldName, metaType, true ) );
    avroSchemaDescription.addField( avroSchemaDescription.new Field( formatFieldName, pentahoFieldName, avroType, true ) );

    RowMetaAndData rowMetaAndData = mock( RowMetaAndData.class );
    AvroConverter.convertFromAvro( rowMetaAndData, record, avroSchemaDescription, metaSchemaDescription );

    verify( rowMetaAndData ).addValue( pentahoFieldName, metaType, expected );
  }

}
