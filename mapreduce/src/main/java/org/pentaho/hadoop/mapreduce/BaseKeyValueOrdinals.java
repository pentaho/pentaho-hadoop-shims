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

package org.pentaho.hadoop.mapreduce;

import org.pentaho.di.core.row.RowMetaInterface;

public abstract class BaseKeyValueOrdinals {
  private int keyOrdinal = -1;
  private int valueOrdinal = -1;

  public BaseKeyValueOrdinals( RowMetaInterface rowMeta ) {
    if ( rowMeta != null ) {
      String[] fieldNames = rowMeta.getFieldNames();

      for ( int i = 0; i < fieldNames.length; i++ ) {
        if ( fieldNames[ i ].equalsIgnoreCase( getKeyName() ) ) {
          keyOrdinal = i;
          if ( valueOrdinal >= 0 ) {
            break;
          }
        } else if ( fieldNames[ i ].equalsIgnoreCase( getValueName() ) ) {
          valueOrdinal = i;
          if ( keyOrdinal >= 0 ) {
            break;
          }
        }
      }
    }
  }

  protected abstract String getKeyName();

  protected abstract String getValueName();

  public int getKeyOrdinal() {
    return keyOrdinal;
  }

  public int getValueOrdinal() {
    return valueOrdinal;
  }
}
