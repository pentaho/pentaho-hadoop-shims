/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2013 by Pentaho : http://www.pentaho.com
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

package org.pentaho.hadoop.mapreduce;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.RowProducer;
import org.pentaho.di.trans.SingleThreadedTransExecutor;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.hadoop.mapreduce.converter.TypeConverterFactory;
import org.pentaho.hadoop.mapreduce.converter.spi.ITypeConverter;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * A reducer class that just emits the sum of the input values.
 */
@SuppressWarnings( "deprecation" )
public class GenericTransReduce<K extends WritableComparable<?>, V extends Iterator<Writable>, K2, V2>
  extends PentahoMapReduceBase<K2, V2> implements
  Reducer<K, V, K2, V2> {

  protected RowProducer rowProducer;
  protected Object value;
  protected InKeyValueOrdinals inOrdinals = null;
  protected TypeConverterFactory typeConverterFactory;
  protected ITypeConverter inConverterK = null;
  protected ITypeConverter inConverterV = null;
  protected RowMetaInterface injectorRowMeta;
  protected SingleThreadedTransExecutor executor;

  public GenericTransReduce() throws KettleException {
    super();
    this.setMRType( MROperations.Reduce );
    typeConverterFactory = new TypeConverterFactory();
  }

  public boolean isSingleThreaded() {
    return reduceSingleThreaded;
  }

  public String getInputStepName() {
    return reduceInputStepName;
  }

  public String getOutputStepName() {
    return reduceOutputStepName;
  }

  public void reduce( final K key, final Iterator<V> values, final OutputCollector<K2, V2> output,
                      final Reporter reporter ) throws IOException {
    try {
      if ( trans == null ) {
        throw new RuntimeException( "Error initializing transformation.  See error log." ); //$NON-NLS-1$
      }

      if ( !trans.isRunning() ) {
        shareVariableSpaceWithTrans( reporter );
        setTransLogLevel( reporter );
        prepareExecution( reporter );
        addInjectorAndProducerToTrans( key, values, output, reporter, getInputStepName(), getOutputStepName() );

        if ( isSingleThreaded() ) {
          executor = new SingleThreadedTransExecutor( trans );
          boolean ok = executor.init();
          if ( !ok ) {
            throw new KettleException(
              "Unable to initialize the single threaded transformation, check the log for details." );
          }
        }
      }

      injectValues( key, values, reporter );

      if ( isSingleThreaded() ) {
        executor.oneIteration();
      }
    } catch ( Exception e ) {
      printException( reporter, e );
      setDebugStatus( reporter, "An exception was raised" );
      throw new IOException( e );
    }
  }

  private void printException( Reporter reporter, Exception e ) throws IOException {
    e.printStackTrace( System.err );
    setDebugStatus( reporter, "An exception was raised" );
    throw new IOException( e );

  }

  private void disposeTransformation() {
    try {
      trans.stopAll();
    } catch ( Exception ex ) {
      ex.printStackTrace();
    }
    try {
      trans.cleanup();
    } catch ( Exception ex ) {
      ex.printStackTrace();
    }
  }

  private void injectValues( final K key, final Iterator<V> values, final Reporter reporter ) throws Exception {
    if ( rowProducer != null ) {

      if ( value != null ) {
        if ( inOrdinals != null ) {
          injectValue( key, inOrdinals.getKeyOrdinal(), inConverterK, value, inOrdinals.getValueOrdinal(), inConverterV,
            injectorRowMeta, rowProducer, reporter );
        } else {
          injectValue( key, inConverterK, value, inConverterV, injectorRowMeta, rowProducer, reporter );
        }
      }

      while ( values.hasNext() ) {
        value = values.next();

        if ( inOrdinals != null ) {
          injectValue( key, inOrdinals.getKeyOrdinal(), inConverterK, value, inOrdinals.getValueOrdinal(), inConverterV,
            injectorRowMeta, rowProducer, reporter );
        } else {
          injectValue( key, inConverterK, value, inConverterV, injectorRowMeta, rowProducer, reporter );
        }
      }

      value = null;
    }
  }

  private void prepareExecution( Reporter reporter ) throws KettleException {
    setDebugStatus( reporter, "Preparing transformation for execution" );
    trans.prepareExecution( null );
  }


  /**
   * set the trans' log level if we have our's set
   *
   * @param reporter
   */
  private void setTransLogLevel( Reporter reporter ) {
    if ( logLevel != null ) {
      setDebugStatus( reporter, "Setting the trans.logLevel to " + logLevel.toString() );
      trans.setLogLevel( logLevel );
    } else {
      setDebugStatus( reporter, getClass().getName() + ".logLevel is null.  The trans log level will not be set." );
    }
  }

  /**
   * share the variables from the PDI job. we do this here instead of in createTrans() as MRUtil.recreateTrans() will
   * not copy "execution" trans information.
   */
  private void shareVariableSpaceWithTrans( Reporter reporter ) {
    if ( variableSpace != null ) {
      setDebugStatus( reporter, "Sharing the VariableSpace from the PDI job." );
      trans.shareVariablesWith( variableSpace );

      if ( debug ) {

        //  list the variables
        List<String> variables = Arrays.asList( trans.listVariables() );
        Collections.sort( variables );

        if ( variables != null ) {
          setDebugStatus( reporter, "Variables: " );
          for ( String variable : variables ) {
            setDebugStatus( reporter, "     " + variable + " = " + trans.getVariable( variable ) );
          }
        }
      }
    } else {
      setDebugStatus( reporter, "variableSpace is null.  We are not going to share it with the trans." );
    }

  }

  private void addInjectorAndProducerToTrans( K key, Iterator<V> values, OutputCollector<K2, V2> output,
                                              Reporter reporter, String inputStepName, String outputStepName )
    throws Exception {
    setDebugStatus( reporter, "Locating output step: " + outputStepName );
    StepInterface outputStep = trans.findRunThread( outputStepName );
    if ( outputStep != null ) {
      rowCollector = new OutputCollectorRowListener( output, outClassK, outClassV, reporter, debug );
      outputStep.addRowListener( rowCollector );

      injectorRowMeta = new RowMeta();
      setDebugStatus( reporter, "Locating input step: " + inputStepName );
      if ( inputStepName != null ) {
        // Setup row injection
        rowProducer = trans.addRowProducer( inputStepName, 0 );
        StepInterface inputStep = rowProducer.getStepInterface();
        StepMetaInterface inputStepMeta = inputStep.getStepMeta().getStepMetaInterface();

        inOrdinals = null;
        if ( inputStepMeta instanceof BaseStepMeta ) {
          setDebugStatus( reporter, "Generating converters from RowMeta for injection into the transformation" );

          // Convert to BaseStepMeta and use getFields(...) to get the row meta and therefore the expected input types
          ( (BaseStepMeta) inputStepMeta ).getFields( injectorRowMeta, null, null, null, null );

          inOrdinals = new InKeyValueOrdinals( injectorRowMeta );

          if ( inOrdinals.getKeyOrdinal() < 0 || inOrdinals.getValueOrdinal() < 0 ) {
            throw new KettleException( "key or value is not defined in transformation injector step" );
          }

          // Get a converter for the Key if the value meta has a concrete Java class we can use.
          // If no converter can be found here we wont do any type conversion.
          if ( injectorRowMeta.getValueMeta( inOrdinals.getKeyOrdinal() ) != null ) {
            inConverterK = typeConverterFactory
              .getConverter( key.getClass(), injectorRowMeta.getValueMeta( inOrdinals.getKeyOrdinal() ) );
          }

          // we need to peek into the first value to get the class (the combination of Iterator and generic makes
          // this a pain)
          if ( values.hasNext() ) {
            value = values.next();
          }
          if ( value != null ) {
            // Get a converter for the Value if the value meta has a concrete Java class we can use.
            // If no converter can be found here we wont do any type conversion.
            if ( injectorRowMeta.getValueMeta( inOrdinals.getValueOrdinal() ) != null ) {
              inConverterV = typeConverterFactory
                .getConverter( value.getClass(), injectorRowMeta.getValueMeta( inOrdinals.getValueOrdinal() ) );
            }
          }
        }

        trans.startThreads();
      } else {
        setDebugStatus( reporter, "No input stepname was defined" );
      }

      if ( getException() != null ) {
        setDebugStatus( reporter, "An exception was generated by the transformation" );
        // Bubble the exception from within Kettle to Hadoop
        throw getException();
      }

    } else {
      if ( outputStepName != null ) {
        setDebugStatus( reporter, "Output step [" + outputStepName + "] could not be found" );
        throw new KettleException( "Output step not defined in transformation" );
      } else {
        setDebugStatus( reporter, "Output step name not specified" );
      }
    }
  }

  @Override
  public void close() throws IOException {
    rowProducer.finished();

    if ( isSingleThreaded() && executor != null ) {
      try {
        //executor.oneIteration();
        executor.dispose();
      } catch ( KettleException e ) {
        e.printStackTrace( System.err );
        trans.getLogChannel().logError( "Error disposing of single threading transformation: ", e );
      }
    } else if ( !isSingleThreaded() && trans != null ) {
      trans.waitUntilFinished();
      disposeTransformation();
    }

    super.close();
  }
}
