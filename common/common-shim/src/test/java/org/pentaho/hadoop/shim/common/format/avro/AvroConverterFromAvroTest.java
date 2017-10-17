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
  private int type;
  private Object value;
  private Object expected;

  public AvroConverterFromAvroTest( String formatFieldName, String pentahoFieldName, int type, Object value,
                                    Object expected ) {
    this.formatFieldName = formatFieldName;
    this.pentahoFieldName = pentahoFieldName;
    this.type = type;
    this.value = value;
    this.expected = expected;
  }

  @Parameterized.Parameters
  public static Collection values() {
    return Arrays.asList( new Object[][] {
      { "inetFormatField", "inetPentahoField", ValueMetaInterface.TYPE_INET, "inetData", "inetData" },
      { "stringFormatField", "stringPentahoField", ValueMetaInterface.TYPE_STRING, "stringData", "stringData" },
      { "integerFormatField", "integerPentahoField", ValueMetaInterface.TYPE_INTEGER, 5, 5 },
      { "numberFormatField", "numberPentahoField", ValueMetaInterface.TYPE_NUMBER, 7f, 7.0 },
      { "bigNumberFormatField", "bigNumberPentahoField", ValueMetaInterface.TYPE_BIGNUMBER, "77d", new BigDecimal( 77d ) },
      { "tsFormatField", "tsPentahoField", ValueMetaInterface.TYPE_TIMESTAMP, 10L, new Timestamp( 10L ) },
      { "dateFormatField", "datePentahoField", ValueMetaInterface.TYPE_DATE,
        1, Date.from( LocalDate.ofEpochDay( 1 ).atStartOfDay( ZoneId.systemDefault() ).toInstant() ) },
      { "booleanFormatField", "booleanPentahoField", ValueMetaInterface.TYPE_BOOLEAN, true, true },
      { "binaryFormatField", "binaryPentahoField", ValueMetaInterface.TYPE_BINARY,
        ByteBuffer.wrap( new byte[] {1, 2, 3} ), new byte[] {1, 2, 3} }
    } );
  }


  @Test
  public void convertFromAvro() throws Exception {
    GenericRecord record = mock( GenericRecord.class );
    when( record.get( formatFieldName ) ).thenReturn( value );
    SchemaDescription schemaDescription = new SchemaDescription();
    schemaDescription.addField( schemaDescription.new Field( formatFieldName, pentahoFieldName, type, true ) );

    RowMetaAndData rowMetaAndData = mock( RowMetaAndData.class );
    AvroConverter.convertFromAvro( rowMetaAndData, record, schemaDescription );

    verify( rowMetaAndData ).addValue( pentahoFieldName, type, expected );
  }

}
