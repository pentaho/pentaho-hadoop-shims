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


package org.pentaho.hadoop.mapreduce;

import org.junit.Assert;
import org.junit.Test;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;

import static org.junit.Assert.assertEquals;

public class OrdinalExtractionTest {

  private RowMetaInterface generateRowMeta( String[] fieldNames ) {
    RowMetaInterface rowMeta = new RowMeta();

    for ( String fieldName : fieldNames ) {
      ValueMetaInterface col = new ValueMeta();
      col.setName( fieldName );
      col.setType( ValueMeta.TYPE_STRING );
      rowMeta.addValueMeta( col );
    }
    return rowMeta;
  }

  @Test
  public void invalidFields() {
    RowMetaInterface meta = generateRowMeta( new String[] { "valueOne", "valueTwo", "valueThree", "valueFour" } );
    test( "Invalid Fields", InKeyValueOrdinals.class, meta, -1, -1 );
    test( "Invalid Fields", OutKeyValueOrdinals.class, meta, -1, -1 );
  }

  @Test
  public void noFields() {
    RowMetaInterface meta = generateRowMeta( new String[] {} );
    test( "No Fields", InKeyValueOrdinals.class, meta, -1, -1 );
    test( "No Fields", OutKeyValueOrdinals.class, meta, -1, -1 );
  }

  @Test
  public void inFieldsFirst() {
    RowMetaInterface meta = generateRowMeta( new String[] { "key", "value", "valueThree", "valueFour" } );
    test( "In Fields First", InKeyValueOrdinals.class, meta, 0, 1 );
    test( "In Fields First", OutKeyValueOrdinals.class, meta, -1, -1 );
  }

  @Test
  public void inFieldsLast() {
    RowMetaInterface meta = generateRowMeta( new String[] { "valueOne", "valueTwo", "value", "key" } );
    test( "In Fields Last", InKeyValueOrdinals.class, meta, 3, 2 );
    test( "In Fields Last", OutKeyValueOrdinals.class, meta, -1, -1 );
  }

  @Test
  public void outFieldsFirst() {
    RowMetaInterface meta = generateRowMeta( new String[] { "outKey", "outValue", "valueThree", "valueFour" } );
    test( "Out Fields First", InKeyValueOrdinals.class, meta, -1, -1 );
    test( "Out Fields First", OutKeyValueOrdinals.class, meta, 0, 1 );
  }

  @Test
  public void outFieldsLast() {
    RowMetaInterface meta = generateRowMeta( new String[] { "valueOne", "valueTwo", "outValue", "outKey" } );
    ;
    test( "Out Fields Last", InKeyValueOrdinals.class, meta, -1, -1 );
    test( "Out Fields Last", OutKeyValueOrdinals.class, meta, 3, 2 );
  }

  @Test
  public void oneInOneOutValueField() {
    RowMetaInterface meta = generateRowMeta( new String[] { "valueOne", "valueTwo", "outValue", "value" } );
    test( "One In One Out Value Field", InKeyValueOrdinals.class, meta, -1, 3 );
    test( "One In One Out Value Field", OutKeyValueOrdinals.class, meta, -1, 2 );
  }

  @Test
  public void oneInOneOutKeyField() {
    RowMetaInterface meta = generateRowMeta( new String[] { "valueOne", "outKey", "key", "valueFour" } );
    test( "One In One Out Key Field", InKeyValueOrdinals.class, meta, 2, -1 );
    test( "One In One Out Key Field", OutKeyValueOrdinals.class, meta, 1, -1 );
  }

  private void test( String testName, Class<?> keyValueOrdinalClass, RowMetaInterface rowMeta, int expectedKey,
                     int expectedValue ) {
    BaseKeyValueOrdinals ordinals;
    try {
      ordinals =
        (BaseKeyValueOrdinals) keyValueOrdinalClass.getConstructor( RowMetaInterface.class ).newInstance( rowMeta );

      assertEquals( testName + ": key", expectedKey, ordinals.getKeyOrdinal() );
      assertEquals( testName + ": value", expectedValue, ordinals.getValueOrdinal() );

    } catch ( Exception e ) {
      Assert
        .assertTrue( "Unexpected exception creating class [" + keyValueOrdinalClass.getName() + "] from constructor",
          false );
    }
  }
}
