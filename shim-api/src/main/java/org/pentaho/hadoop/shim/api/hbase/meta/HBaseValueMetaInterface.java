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

package org.pentaho.hadoop.shim.api.hbase.meta;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;

/**
 * Created by bryan on 1/19/16.
 */
public interface HBaseValueMetaInterface extends ValueMetaInterface {
  boolean isKey();

  void setKey( boolean key );

  String getAlias();

  void setAlias( String alias );

  String getColumnName();

  void setColumnName( String columnName );

  String getColumnFamily();

  void setColumnFamily( String family );

  void setHBaseTypeFromString( String hbaseType ) throws IllegalArgumentException;

  String getHBaseTypeDesc();

  Object decodeColumnValue( byte[] rawColValue ) throws KettleException;

  String getTableName();

  void setTableName( String tableName );

  String getMappingName();

  void setMappingName( String mappingName );

  boolean getIsLongOrDouble();

  void setIsLongOrDouble( boolean ld );

  byte[] encodeColumnValue( Object o, ValueMetaInterface valueMetaInterface ) throws KettleException;

  void getXml( StringBuilder stringBuilder );

  void saveRep( Repository rep, ObjectId id_transformation, ObjectId id_step, int count ) throws KettleException;
}
