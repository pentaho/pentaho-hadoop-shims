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


package com.pentaho.big.data.bundles.impl.shim.hbase.mapping;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.hadoop.shim.api.hbase.mapping.ColumnFilter;

/**
 * Created by bryan on 1/21/16.
 */
public class ColumnFilterImpl implements ColumnFilter {
  private final org.pentaho.hadoop.shim.api.internal.hbase.ColumnFilter delegate;

  public ColumnFilterImpl( org.pentaho.hadoop.shim.api.internal.hbase.ColumnFilter delegate ) {
    this.delegate = delegate;
  }

  @Override public String getFieldAlias() {
    return delegate.getFieldAlias();
  }

  @Override public void setFieldAlias( String alias ) {
    delegate.setFieldAlias( alias );
  }

  @Override public String getFieldType() {
    return delegate.getFieldType();
  }

  @Override public void setFieldType( String type ) {
    delegate.setFieldType( type );
  }

  @Override public ComparisonType getComparisonOperator() {
    org.pentaho.hadoop.shim.api.internal.hbase.ColumnFilter.ComparisonType comparisonOperator =
      delegate.getComparisonOperator();
    if ( comparisonOperator == null ) {
      return null;
    }
    return ComparisonType.valueOf( comparisonOperator.name() );
  }

  @Override public void setComparisonOperator( ComparisonType c ) {
    if ( c == null ) {
      delegate.setComparisonOperator( null );
    } else {
      delegate.setComparisonOperator(
        org.pentaho.hadoop.shim.api.internal.hbase.ColumnFilter.ComparisonType.valueOf( c.name() ) );
    }
  }

  @Override public boolean getSignedComparison() {
    return delegate.getSignedComparison();
  }

  @Override public void setSignedComparison( boolean signed ) {
    delegate.setSignedComparison( signed );
  }

  @Override public String getConstant() {
    return delegate.getConstant();
  }

  @Override public void setConstant( String constant ) {
    delegate.setConstant( constant );
  }

  @Override public String getFormat() {
    return delegate.getFormat();
  }

  @Override public void setFormat( String format ) {
    delegate.setFormat( format );
  }

  @Override public void appendXML( StringBuilder buff ) {
    StringBuffer stringBuffer = new StringBuffer();
    delegate.appendXML( stringBuffer );
    buff.append( stringBuffer );
  }

  @Override public void saveRep( Repository rep, ObjectId id_transformation, ObjectId id_step, int filterNum )
    throws KettleException {
    delegate.saveRep( rep, id_transformation, id_step, filterNum );
  }
}
