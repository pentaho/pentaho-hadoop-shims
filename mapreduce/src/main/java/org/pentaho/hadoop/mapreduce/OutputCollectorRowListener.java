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

import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.RowAdapter;
import org.pentaho.hadoop.mapreduce.PentahoMapRunnable.Counter;
import org.pentaho.hadoop.mapreduce.converter.TypeConverterFactory;
import org.pentaho.hadoop.mapreduce.converter.spi.ITypeConverter;

import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.logging.LogChannelInterface;

/**
 * Row Listener that forwards rows along to an {@link OutputCollector}.
 */
public class OutputCollectorRowListener<K, V> extends RowAdapter {

  private static LogChannelInterface log = new LogChannel( OutputCollectorRowListener.class.getName() );

  private boolean debug;

  private Reporter reporter;

  private Class<K> outClassK;

  private Class<V> outClassV;

  private OutputCollector<K, V> output;

  private Exception exception;

  private OutKeyValueOrdinals outOrdinals;

  private TypeConverterFactory typeConverterFactory;

  public OutputCollectorRowListener( OutputCollector<K, V> output, Class<K> outClassK, Class<V> outClassV,
                                     Reporter reporter, boolean debug ) {
    this.output = output;
    this.outClassK = outClassK;
    this.outClassV = outClassV;
    this.reporter = reporter;
    this.debug = debug;

    this.typeConverterFactory = new TypeConverterFactory();

    outOrdinals = null;
  }

  @Override
  public void rowWrittenEvent( RowMetaInterface rowMeta, Object[] row ) throws KettleStepException {
    try {
      /*
       * Operation:
       * Column 1: Key (convert to outClassK)
       * Column 2: Value (convert to outClassV)
       */
      if ( row != null && !rowMeta.isEmpty() && rowMeta.size() >= 2 ) {
        if ( outOrdinals == null ) {
          outOrdinals = new OutKeyValueOrdinals( rowMeta );

          if ( outOrdinals.getKeyOrdinal() < 0 || outOrdinals.getValueOrdinal() < 0 ) {
            throw new KettleException(
              "outKey or outValue is not defined in transformation output stream" ); //$NON-NLS-1$
          }
        }

        // TODO Implement type safe converters

        if ( log.isDebug() ) {
          setDebugStatus( reporter,
            "Begin conversion of output key [from:" + ( row[ outOrdinals.getKeyOrdinal() ] == null ? null
              : row[ outOrdinals.getKeyOrdinal() ].getClass() ) + "] [to:" + outClassK
              + "]" ); //$NON-NLS-1$ //$NON-NLS-2$
        }
        if ( log.isDebug() ) {
          setDebugStatus( reporter, "getConverter: " + ( row[ outOrdinals.getKeyOrdinal() ] == null ? null
            : row[ outOrdinals.getKeyOrdinal() ].getClass() ) );
          setDebugStatus( reporter, "out class: " + outClassK );
        }

        ITypeConverter converter = typeConverterFactory
          .getConverter(
            row[ outOrdinals.getKeyOrdinal() ] == null ? null : row[ outOrdinals.getKeyOrdinal() ].getClass(),
            outClassK );
        if ( log.isDebug() ) {
          setDebugStatus( reporter, "ordinals key: " + outOrdinals.getKeyOrdinal() );
          setDebugStatus( reporter, "rowMeta: " + rowMeta );
          setDebugStatus( reporter, "rowMeta: " + rowMeta.getMetaXML() );
          setDebugStatus( reporter, "meta: " + rowMeta.getValueMeta( outOrdinals.getKeyOrdinal() ) );
          setDebugStatus( reporter, "key: " + row[ outOrdinals.getKeyOrdinal() ] );
        }

        Object outKey =
          converter.convert( rowMeta.getValueMeta( outOrdinals.getKeyOrdinal() ), row[ outOrdinals.getKeyOrdinal() ] );

        if ( log.isDebug() ) {
          setDebugStatus( reporter,
            "Begin conversion of output value [from:" + ( row[ outOrdinals.getValueOrdinal() ] == null ? null
              //$NON-NLS-1$
              : row[ outOrdinals.getValueOrdinal() ].getClass() ) + "] [to:" + outClassV
              + "]" ); //$NON-NLS-1$ //$NON-NLS-2$
        }
        ITypeConverter valueConverter = typeConverterFactory.getConverter(
          row[ outOrdinals.getValueOrdinal() ] == null ? null : row[ outOrdinals.getValueOrdinal() ].getClass(),
          outClassV );
        if ( log.isDebug() ) {
          setDebugStatus( reporter, "ordinals value: " + outOrdinals.getValueOrdinal() );
          setDebugStatus( reporter, "rowMeta: " + rowMeta );
          setDebugStatus( reporter, "rowMeta: " + rowMeta.getMetaXML() );
          setDebugStatus( reporter, "meta: " + rowMeta.getValueMeta( outOrdinals.getValueOrdinal() ) );
          setDebugStatus( reporter, "value: " + row[ outOrdinals.getValueOrdinal() ] );
        }
        Object outVal = valueConverter
          .convert( rowMeta.getValueMeta( outOrdinals.getValueOrdinal() ), row[ outOrdinals.getValueOrdinal() ] );

        if ( outKey != null && outVal != null ) {
          if ( log.isDebug() ) {
            setDebugStatus( reporter,
              "Collecting output record [" + outKey + "] - [" + outVal
                + "]" ); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
          }
          // TODO Implement type safe converters
          @SuppressWarnings( "unchecked" )
          K k = (K) outKey;
          // TODO Implement type safe converters
          @SuppressWarnings( "unchecked" )
          V v = (V) outVal;
          output.collect( k, v );
        } else {
          if ( outKey == null ) {
            if ( log.isDebug() ) {
              setDebugStatus( reporter, "Transformation returned a null key" ); //$NON-NLS-1$
            }
            reporter.incrCounter( Counter.OUT_RECORD_WITH_NULL_KEY, 1 );
          }
          if ( outVal == null ) {
            if ( log.isDebug() ) {
              setDebugStatus( reporter, "Transformation returned a null value" ); //$NON-NLS-1$
            }
            reporter.incrCounter( Counter.OUT_RECORD_WITH_NULL_VALUE, 1 );
          }
        }
      } else {
        if ( row == null || rowMeta.isEmpty() ) {
          if ( log.isDebug() ) {
            setDebugStatus( reporter, "Invalid row received from transformation" ); //$NON-NLS-1$
          }
        } else if ( rowMeta.size() < 2 ) {
          if ( log.isDebug() ) {
            setDebugStatus( reporter,
              "Invalid row format. Expected key/value columns, but received " + rowMeta.size() //$NON-NLS-1$
                + " columns" ); //$NON-NLS-1$
          }
        } else {
          OutKeyValueOrdinals outOrdinals = new OutKeyValueOrdinals( rowMeta );
          if ( ( outOrdinals.getKeyOrdinal() < 0 || outOrdinals.getValueOrdinal() < 0 ) && log.isDebug() ) {
            setDebugStatus( reporter,
              "outKey or outValue is missing from the transformation output step" ); //$NON-NLS-1$
          }
          if ( log.isDebug() ) {
            setDebugStatus( reporter, "Unknown issue with received data from transformation" ); //$NON-NLS-1$
          }
        }
      }
    } catch ( Exception ex ) {
      setDebugStatus( reporter, "Unexpected exception recieved: " + ex.getMessage() ); //$NON-NLS-1$
      exception = ex;
      throw new RuntimeException( ex );
    }
  }

  /**
   * Set the reporter status if {@code debug == true}.
   */
  public void setDebugStatus( Reporter reporter, String message ) {
    if ( log.isDebug() ) {
      log.logDebug( message );
      reporter.setStatus( message );
    }
  }

  /**
   * @return The exception thrown from {@link #rowWrittenEvent(RowMetaInterface, Object[])}.
   */
  public Exception getException() {
    return exception;
  }
}
