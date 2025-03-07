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


package com.pentaho.big.data.bundles.impl.shim.hbase.meta;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.hadoop.shim.api.hbase.meta.HBaseValueMetaInterface;
import org.pentaho.hadoop.shim.api.internal.hbase.HBaseBytesUtilShim;
import org.pentaho.hadoop.shim.api.internal.hbase.HBaseValueMeta;


/**
 * Created by bryan on 1/22/16.
 */
public class HBaseValueMetaInterfaceImpl extends HBaseValueMeta implements HBaseValueMetaInterface {
  private final HBaseBytesUtilShim hBaseBytesUtilShim;

  public HBaseValueMetaInterfaceImpl( String name, int type, int length, int precision,
                                      HBaseBytesUtilShim hBaseBytesUtilShim )
    throws IllegalArgumentException {
    super( name, type, length, precision );
    this.hBaseBytesUtilShim = hBaseBytesUtilShim;
  }

  @Override public Object decodeColumnValue( byte[] rawColValue ) throws KettleException {
    return HBaseValueMeta.decodeColumnValue( rawColValue, this, hBaseBytesUtilShim );
  }

  @Override public byte[] encodeColumnValue( Object o, ValueMetaInterface valueMetaInterface ) throws KettleException {
    return HBaseValueMeta.encodeColumnValue( o, valueMetaInterface, this, hBaseBytesUtilShim );
  }

  @Override public void getXml( StringBuilder retval ) {
    retval.append( "\n        " ).append( XMLHandler.openTag( "field" ) );
    retval.append( "\n            " ).append( XMLHandler.addTagValue( "table_name", getTableName() ) );
    retval.append( "\n            " ).append( XMLHandler.addTagValue( "mapping_name", getMappingName() ) );
    retval.append( "\n            " ).append( XMLHandler.addTagValue( "alias", getAlias() ) );
    retval.append( "\n            " ).append( XMLHandler.addTagValue( "family", getColumnFamily() ) );
    retval.append( "\n            " ).append( XMLHandler.addTagValue( "column", getColumnName() ) );
    retval.append( "\n            " ).append( XMLHandler.addTagValue( "key", isKey() ) );
    retval.append( "\n            " ).append(
      XMLHandler.addTagValue( "type", ValueMeta.getTypeDesc( getType() ) ) );
    String format = getConversionMask();
    retval.append( "\n            " ).append( XMLHandler.addTagValue( "format", format ) );
    if ( getStorageType() == ValueMetaInterface.STORAGE_TYPE_INDEXED ) {
      retval.append( "\n            " ).append( XMLHandler.addTagValue( "index_values", getIndexValues() ) );
    }
    retval.append( "\n        " ).append( XMLHandler.closeTag( "field" ) );
  }

  @Override public void saveRep( Repository rep, ObjectId id_transformation, ObjectId id_step, int i )
    throws KettleException {
    rep.saveStepAttribute( id_transformation, id_step, i, "table_name", getTableName() );
    rep.saveStepAttribute( id_transformation, id_step, i, "mapping_name", getMappingName() );
    rep.saveStepAttribute( id_transformation, id_step, i, "alias", getAlias() );
    rep.saveStepAttribute( id_transformation, id_step, i, "family", getColumnFamily() );
    rep.saveStepAttribute( id_transformation, id_step, i, "column", getColumnName() );
    rep.saveStepAttribute( id_transformation, id_step, i, "key", isKey() );
    rep.saveStepAttribute( id_transformation, id_step, i, "type", ValueMeta.getTypeDesc( getType() ) );
    String format = getConversionMask();
    rep.saveStepAttribute( id_transformation, id_step, i, "format", format );
    if ( getStorageType() == ValueMetaInterface.STORAGE_TYPE_INDEXED ) {
      rep.saveStepAttribute( id_transformation, id_step, i, "index_values", getIndexValues() );
    }
  }

  private String getIndexValues() {
    Object[] labels = getIndex();
    StringBuffer vals = new StringBuffer();
    vals.append( "{" );

    for ( int i = 0; i < labels.length; i++ ) {
      if ( i != labels.length - 1 ) {
        vals.append( labels[ i ].toString().trim() ).append( "," );
      } else {
        vals.append( labels[ i ].toString().trim() ).append( "}" );
      }
    }
    return vals.toString();
  }
}
