/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

import com.thoughtworks.xstream.XStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapRunnable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.KettleLogStore;
import org.pentaho.di.core.logging.KettleLoggingEvent;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.trans.RowProducer;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.steps.missing.MissingTrans;
import org.pentaho.hadoop.mapreduce.converter.TypeConverterFactory;
import org.pentaho.hadoop.mapreduce.converter.spi.ITypeConverter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Map runner that uses the normal Kettle execution engine to process all input data during one single run.<p> This
 * relies on newly un-@Deprecated interfaces ({@link MapRunnable}, {@link JobConf}) in Hadoop 0.21.0.
 */
public class PentahoMapRunnable<K1, V1, K2, V2> implements MapRunnable<K1, V1, K2, V2> {
  public static final String KETTLE_PMR_PLUGIN_TIMEOUT = "KETTLE_PMR_PLUGIN_TIMEOUT";

  private long pluginWaitTimeout;

  protected static enum Counter {
    INPUT_RECORDS, OUTPUT_RECORDS, OUT_RECORD_WITH_NULL_KEY, OUT_RECORD_WITH_NULL_VALUE
  }
  protected String transMapXml;

  protected String transReduceXml;

  protected String mapInputStepName;

  protected String reduceInputStepName;

  protected String mapOutputStepName;

  protected String reduceOutputStepName;

  protected Class<K2> outClassK;

  protected Class<V2> outClassV;

  protected String id = UUID.randomUUID().toString();

  protected boolean debug = false;

  //  the transformation that will be used as a mapper or reducer
  protected Trans trans;

  protected VariableSpace variableSpace = null;

  protected LogLevel logLevel;

  protected OutputCollectorRowListener<K2, V2> rowCollector;

  private final String ENVIRONMENT_VARIABLE_PREFIX = "java.system.";
  private final String KETTLE_VARIABLE_PREFIX = "KETTLE_";

  public PentahoMapRunnable() throws KettleException {
  }

  public void configure( JobConf job ) {
    pluginWaitTimeout = TimeUnit.MINUTES.toMillis( 5 );

    debug = "true".equalsIgnoreCase( job.get( "debug" ) ); //$NON-NLS-1$

    transMapXml = job.get( "transformation-map-xml" );
    transReduceXml = job.get( "transformation-reduce-xml" );
    mapInputStepName = job.get( "transformation-map-input-stepname" );
    mapOutputStepName = job.get( "transformation-map-output-stepname" );
    reduceInputStepName = job.get( "transformation-reduce-input-stepname" );
    reduceOutputStepName = job.get( "transformation-reduce-output-stepname" );
    String xmlVariableSpace = job.get( "variableSpace" );

    outClassK = (Class<K2>) job.getMapOutputKeyClass();
    outClassV = (Class<V2>) job.getMapOutputValueClass();

    if ( !Const.isEmpty( xmlVariableSpace ) ) {
      setDebugStatus( "PentahoMapRunnable(): variableSpace was retrieved from the job.  The contents: " );
      setDebugStatus( xmlVariableSpace );

      //  deserialize from xml to variable space
      XStream xStream = new XStream();

      setDebugStatus( "PentahoMapRunnable(): Setting classes variableSpace property.: " );
      variableSpace = (VariableSpace) xStream.fromXML( xmlVariableSpace );

      for ( String variableName : variableSpace.listVariables() ) {
        if ( variableName.startsWith( KETTLE_VARIABLE_PREFIX ) ) {
          System.setProperty( variableName, variableSpace.getVariable( variableName ) );
        }
        if ( KETTLE_PMR_PLUGIN_TIMEOUT.equals( variableName ) ) {
          try {
            pluginWaitTimeout = Long.parseLong( variableSpace.getVariable( variableName ) );
          } catch ( Exception e ) {
            System.out.println( "Unable to parse plugin wait timeout, defaulting to 5 minutes" );
          }
        }
      }
    } else {
      setDebugStatus( "PentahoMapRunnable(): The PDI Job's variable space was not sent." );
      variableSpace = new Variables();
    }

    // Check for environment variables in the userDefined variables
    Iterator<Entry<String, String>> iter = job.iterator();
    while ( iter.hasNext() ) {
      Entry<String, String> entry = iter.next();
      if ( entry.getKey().startsWith( ENVIRONMENT_VARIABLE_PREFIX ) ) {
        System.setProperty( entry.getKey().substring( ENVIRONMENT_VARIABLE_PREFIX.length() ), entry.getValue() );
      } else if ( entry.getKey().startsWith( KETTLE_VARIABLE_PREFIX ) ) {
        System.setProperty( entry.getKey(), entry.getValue() );
      }
    }

    MRUtil.passInformationToTransformation( variableSpace, job );

    setDebugStatus( "Job configuration" );
    setDebugStatus( "Output key class: " + outClassK.getName() );
    setDebugStatus( "Output value class: " + outClassV.getName() );

    //  set the log level to what the level of the job is
    String stringLogLevel = job.get( "logLevel" );
    if ( !Const.isEmpty( stringLogLevel ) ) {
      logLevel = LogLevel.valueOf( stringLogLevel );
      setDebugStatus( "Log level set to " + stringLogLevel );
    } else {
      System.out.println( "Could not retrieve the log level from the job configuration.  logLevel will not be set." );
    }

    long deadline = 0;
    boolean first = true;
    while ( true ) {
      createTrans( job );

      if ( first ) {
        deadline = pluginWaitTimeout + System.currentTimeMillis();
        System.out
          .println( PentahoMapRunnable.class + ": Trans creation checking starting now " + new Date().toString() );
        first = false;
      }

      List<MissingTrans> missingTranses = new ArrayList<MissingTrans>();
      for ( StepMeta stepMeta : trans.getTransMeta().getSteps() ) {
        StepMetaInterface stepMetaInterface = stepMeta.getStepMetaInterface();
        if ( stepMetaInterface instanceof MissingTrans ) {
          MissingTrans missingTrans = (MissingTrans) stepMetaInterface;
          System.out.println(
            MissingTrans.class + "{stepName: " + missingTrans.getStepName() + ", missingPluginId: " + missingTrans
              .getMissingPluginId() + "}" );
          missingTranses.add( missingTrans );
        }
      }

      if ( missingTranses.size() == 0 ) {
        System.out.println( PentahoMapRunnable.class + ": Done waiting on plugins now " + new Date().toString() );
        break;
      } else {
        if ( System.currentTimeMillis() > deadline ) {
          StringBuilder stringBuilder = new StringBuilder( "Failed to initialize plugins: " );
          for ( MissingTrans missingTrans : missingTranses ) {
            stringBuilder.append( missingTrans.getMissingPluginId() );
            stringBuilder.append( " on step " ).append( missingTrans.getStepName() );
            stringBuilder.append( ", " );
          }
          stringBuilder.setLength( stringBuilder.length() - 2 );
          throw new RuntimeException( stringBuilder.toString() );
        } else {
          try {
            Thread.sleep( Math.min( 100, deadline - System.currentTimeMillis() ) );
          } catch ( InterruptedException e ) {
            throw new RuntimeException( e );
          }
        }
      }
    }
  }

  public void injectValue( Object key, ITypeConverter inConverterK, Object value, ITypeConverter inConverterV,
                           RowMeta injectorRowMeta, RowProducer rowProducer, Reporter reporter ) throws Exception {

    injectValue( key, 0, inConverterK, value, 1, inConverterV, injectorRowMeta, rowProducer, reporter );
  }

  public void injectValue( Object key, int keyOrdinal, ITypeConverter inConverterK, Object value, int valueOrdinal,
                           ITypeConverter inConverterV, RowMeta injectorRowMeta, RowProducer rowProducer,
                           Reporter reporter )
    throws Exception {
    Object[] row = new Object[ injectorRowMeta.size() ];
    row[ keyOrdinal ] =
      inConverterK != null ? inConverterK.convert( injectorRowMeta.getValueMeta( keyOrdinal ), key ) : key;
    row[ valueOrdinal ] =
      inConverterV != null ? inConverterV.convert( injectorRowMeta.getValueMeta( valueOrdinal ), value )
        : value;

    if ( debug ) {
      setDebugStatus( reporter, "Injecting input record [" + row[ keyOrdinal ] + "] - [" + row[ valueOrdinal ] + "]" );
    }

    rowProducer.putRow( injectorRowMeta, row );
  }

  protected void createTrans( final Configuration conf ) {

    try {
      setDebugStatus( "Creating a transformation for a map." );
      trans = MRUtil.getTrans( conf, transMapXml, false );
    } catch ( KettleException ke ) {
      throw new RuntimeException( "Error loading transformation", ke ); //$NON-NLS-1$
    }
  }

  public String getTransMapXml() {
    return transMapXml;
  }

  public void setTransMapXml( String transMapXml ) {
    this.transMapXml = transMapXml;
  }

  public String getTransReduceXml() {
    return transReduceXml;
  }

  public void setTransReduceXml( String transReduceXml ) {
    this.transReduceXml = transReduceXml;
  }

  public String getMapInputStepName() {
    return mapInputStepName;
  }

  public void setMapInputStepName( String mapInputStepName ) {
    this.mapInputStepName = mapInputStepName;
  }

  public String getMapOutputStepName() {
    return mapOutputStepName;
  }

  public void setMapOutputStepName( String mapOutputStepName ) {
    this.mapOutputStepName = mapOutputStepName;
  }

  public String getReduceInputStepName() {
    return reduceInputStepName;
  }

  public void setReduceInputStepName( String reduceInputStepName ) {
    this.reduceInputStepName = reduceInputStepName;
  }

  public String getReduceOutputStepName() {
    return reduceOutputStepName;
  }

  public void setReduceOutputStepName( String reduceOutputStepName ) {
    this.reduceOutputStepName = reduceOutputStepName;
  }

  public Class<?> getOutClassK() {
    return outClassK;
  }

  public void setOutClassK( Class<K2> outClassK ) {
    this.outClassK = outClassK;
  }

  public Class<?> getOutClassV() {
    return outClassV;
  }

  public void setOutClassV( Class<V2> outClassV ) {
    this.outClassV = outClassV;
  }

  public Trans getTrans() {
    return trans;
  }

  public void setTrans( Trans trans ) {
    this.trans = trans;
  }

  public String getId() {
    return id;
  }

  public void setId( String id ) {
    this.id = id;
  }

  public Exception getException() {
    return rowCollector != null ? rowCollector.getException() : null;
  }

  public void setDebugStatus( Reporter reporter, String message ) {
    if ( debug ) {
      System.out.println( message );
      reporter.setStatus( message );
    }
  }

  private void setDebugStatus( String message ) {
    if ( debug ) {
      System.out.println( message );
    }
  }

  public void run( RecordReader<K1, V1> input, final OutputCollector<K2, V2> output, final Reporter reporter )
    throws IOException {
    try {
      if ( trans == null ) {
        throw new RuntimeException( "Error initializing transformation.  See error log." ); //$NON-NLS-1$
      } else {
        // Clean up old logging
        KettleLogStore.discardLines( trans.getLogChannelId(), true );
      }

      // Create a copy of trans so we don't continue to add new TransListeners and run into a
      // ConcurrentModificationException
      // when this mapper is reused "quickly"
      trans = MRUtil.recreateTrans( trans );

      String logLinePrefix = getClass().getName() + ".run: ";
      setDebugStatus( logLinePrefix + " The transformation was just recreated." );

      //  share the variables from the PDI job.
      //  we do this here instead of in createTrans() as MRUtil.recreateTrans() wil not 
      //  copy "execution" trans information.
      if ( variableSpace != null ) {
        setDebugStatus( "Sharing the VariableSpace from the PDI job." );
        trans.shareVariablesWith( variableSpace );

        if ( debug ) {

          //  list the variables
          List<String> variables = Arrays.asList( trans.listVariables() );
          Collections.sort( variables );

          if ( variables != null ) {
            setDebugStatus( "Variables: " );
            for ( String variable : variables ) {
              setDebugStatus( "     " + variable + " = " + trans.getVariable( variable ) );
            }
          }
        }
      } else {
        setDebugStatus( reporter, "variableSpace is null.  We are not going to share it with the trans." );
      }

      //  set the trans' log level if we have our's set
      if ( logLevel != null ) {
        setDebugStatus( "Setting the trans.logLevel to " + logLevel.toString() );
        trans.setLogLevel( logLevel );
      } else {
        setDebugStatus( "logLevel is null.  The trans log level will not be set." );
      }

      // allocate key & value instances that are re-used for all entries
      K1 key = input.createKey();
      V1 value = input.createValue();

      setDebugStatus( reporter, "Preparing transformation for execution" );
      trans.prepareExecution( null );

      try {
        setDebugStatus( reporter, "Locating output step: " + mapOutputStepName );
        StepInterface outputStep = trans.findRunThread( mapOutputStepName );
        if ( outputStep != null ) {
          rowCollector = new OutputCollectorRowListener( output, outClassK, outClassV, reporter, debug );
          //          rowCollector = OutputCollectorRowListener.build(output, outputRowMeta, outClassK, outClassV,
          // reporter, debug);
          outputStep.addRowListener( rowCollector );

          RowMeta injectorRowMeta = new RowMeta();
          RowProducer rowProducer = null;
          TypeConverterFactory typeConverterFactory = new TypeConverterFactory();
          ITypeConverter inConverterK = null;
          ITypeConverter inConverterV = null;

          setDebugStatus( reporter, "Locating input step: " + mapInputStepName );
          if ( mapInputStepName != null ) {
            // Setup row injection
            rowProducer = trans.addRowProducer( mapInputStepName, 0 );
            StepInterface inputStep = rowProducer.getStepInterface();
            StepMetaInterface inputStepMeta = inputStep.getStepMeta().getStepMetaInterface();

            InKeyValueOrdinals inOrdinals = null;
            if ( inputStepMeta instanceof BaseStepMeta ) {
              setDebugStatus( reporter,
                "Generating converters from RowMeta for injection into the mapper transformation" );

              // Use getFields(...) to get the row meta and therefore the expected input types
              inputStepMeta.getFields( injectorRowMeta, null, null, null, null );

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

              // Get a converter for the Value if the value meta has a concrete Java class we can use. 
              // If no converter can be found here we wont do any type conversion.
              if ( injectorRowMeta.getValueMeta( inOrdinals.getValueOrdinal() ) != null ) {
                inConverterV = typeConverterFactory
                  .getConverter( value.getClass(), injectorRowMeta.getValueMeta( inOrdinals.getValueOrdinal() ) );
              }
            }

            trans.startThreads();
            if ( rowProducer != null ) {

              while ( input.next( key, value ) ) {
                if ( inOrdinals != null ) {
                  injectValue( key, inOrdinals.getKeyOrdinal(), inConverterK, value, inOrdinals.getValueOrdinal(),
                    inConverterV, injectorRowMeta, rowProducer, reporter );
                } else {
                  injectValue( key, inConverterK, value, inConverterV, injectorRowMeta, rowProducer, reporter );
                }
              }

              rowProducer.finished();
            }

            trans.waitUntilFinished();
            setDebugStatus( reporter, "Mapper transformation has finished" );
            if ( trans.getErrors() > 0 ) {
              setDebugStatus( "Errors detected for mapper transformation" );
              List<KettleLoggingEvent> logList = KettleLogStore
                .getLogBufferFromTo( trans.getLogChannelId(), false, 0, KettleLogStore.getLastBufferLineNr() );

              StringBuffer buff = new StringBuffer();
              for ( KettleLoggingEvent le : logList ) {
                if ( le.getLevel() == LogLevel.ERROR ) {
                  buff.append( le.getMessage().toString() ).append( "\n" );
                }
              }
              throw new Exception( "Errors were detected for mapper transformation:\n\n"
                + buff.toString() );
            }

          } else {
            setDebugStatus( reporter, "No input stepname was defined" );
          }
          if ( getException() != null ) {
            setDebugStatus( reporter, "An exception was generated by the mapper transformation" );
            // Bubble the exception from within Kettle to Hadoop
            throw getException();
          }

        } else {
          if ( mapOutputStepName != null ) {
            setDebugStatus( reporter, "Output step [" + mapOutputStepName + "]could not be found" );
            throw new KettleException( "Output step not defined in transformation" );
          } else {
            setDebugStatus( reporter, "Output step name not specified" );
          }
        }
      } finally {
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
    } catch ( Exception e ) {
      e.printStackTrace( System.err );
      setDebugStatus( reporter, "An exception was generated by the mapper task" );
      throw new IOException( e );
    }
    reporter.setStatus( "Completed processing record" );
  }

}
