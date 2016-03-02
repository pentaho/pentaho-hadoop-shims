/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2015 by Pentaho : http://www.pentaho.com
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

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LoggingObjectType;
import org.pentaho.di.core.logging.SimpleLoggingObject;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransConfiguration;
import org.pentaho.di.trans.TransExecutionConfiguration;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.TransMeta.TransformationType;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.steps.missing.MissingTrans;

import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class MRUtil {
  public static final String PROPERTY_PENTAHO_KETTLE_PMR_PLUGIN_TIMEOUT = "pentaho.kettle.pmr.plugin.timeout";

  /**
   * Path to the directory to load plugins from. This must be accessible from all TaskTracker nodes.
   */
  public static final String PROPERTY_PENTAHO_KETTLE_PLUGINS_DIR = "pentaho.kettle.plugins.dir";

  /**
   * Hadoop Configuration for setting KETTLE_HOME. See {@link Const#getKettleDirectory()} for usage.
   */
  public static final String PROPERTY_PENTAHO_KETTLE_HOME = "pentaho.kettle.home";

  public static Trans getTrans( final Configuration conf, final String transXml, boolean singleThreaded )
    throws KettleException {

    initKettleEnvironment( conf );

    TransConfiguration transConfiguration;

    long pluginWaitTimeout = TimeUnit.MINUTES.toMillis( 5 );

    String timeoutString = conf.get( PROPERTY_PENTAHO_KETTLE_PMR_PLUGIN_TIMEOUT );
    if ( timeoutString != null ) {
      try {
        pluginWaitTimeout = Long.parseLong( timeoutString );
      } catch ( Exception e ) {
        System.out.println( BaseMessages.getString( MRUtil.class, "PluginWaiting.CantParse", timeoutString ) );
      }
    }

    long deadline = 0;
    boolean first = true;
    while ( true ) {
      transConfiguration = TransConfiguration.fromXML( transXml );

      if ( first ) {
        deadline = pluginWaitTimeout + System.currentTimeMillis();
        System.out.println(
          BaseMessages.getString( MRUtil.class, "PluginWaiting.Starting", MRUtil.class.getCanonicalName(), new Date().toString() ) );
        first = false;
      }

      List<MissingTrans> missingTranses = new ArrayList<MissingTrans>();
      for ( StepMeta stepMeta : transConfiguration.getTransMeta().getSteps() ) {
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
        System.out
          .println( BaseMessages.getString( MRUtil.class, "PluginWaiting.Done", MRUtil.class.getCanonicalName(), new Date().toString() ) );
        break;
      } else {
        if ( System.currentTimeMillis() > deadline ) {
          StringBuilder stringBuilder =
            new StringBuilder( BaseMessages.getString( MRUtil.class, "PluginWaiting.Failed" ) );
          for ( MissingTrans missingTrans : missingTranses ) {
            stringBuilder.append( missingTrans );
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

    TransMeta transMeta = transConfiguration.getTransMeta();
    String carteObjectId = UUID.randomUUID().toString();
    SimpleLoggingObject servletLoggingObject =
      new SimpleLoggingObject( "HADOOP_MAPPER", LoggingObjectType.CARTE, null ); //$NON-NLS-1$
    servletLoggingObject.setContainerObjectId( carteObjectId );
    TransExecutionConfiguration executionConfiguration = transConfiguration.getTransExecutionConfiguration();
    servletLoggingObject.setLogLevel( executionConfiguration.getLogLevel() );

    if ( singleThreaded ) {
      // Set the type to single threaded in case the user forgot...
      //
      transMeta.setTransformationType( TransformationType.SingleThreaded );

      // Disable thread priority management as it will slow things down needlessly.
      // The single threaded engine doesn't use threads and doesn't need row locking.
      //
      transMeta.setUsingThreadPriorityManagment( false );
    } else {
      transMeta.setTransformationType( TransformationType.Normal );
    }

    return new Trans( transMeta, servletLoggingObject );
  }

  /**
   * Initialize the Kettle environment with settings from the provided configuration
   *
   * @param conf Configuration to configure Kettle environment with
   */
  private static void initKettleEnvironment( Configuration conf ) throws KettleException {
    if ( !KettleEnvironment.isInitialized() ) {
      String kettleHome = getKettleHomeProperty( conf );
      String pluginDir = getPluginDirProperty( conf );
      System.setProperty( "KETTLE_HOME", kettleHome );
      System.setProperty( Const.PLUGIN_BASE_FOLDERS_PROP, pluginDir );

      System.out.println( BaseMessages.getString( MRUtil.class, "KettleHome.Info", kettleHome ) );
      System.out.println( BaseMessages.getString( MRUtil.class, "PluginDirectory.Info", pluginDir ) );

      KettleEnvironment.init();
    }
  }

  public static void passInformationToTransformation( final VariableSpace variableSpace, final JobConf job ) {
    if ( variableSpace != null && job != null ) {
      variableSpace.setVariable( "Internal.Hadoop.NumMapTasks", Integer.toString( job.getNumMapTasks() ) );
      variableSpace.setVariable( "Internal.Hadoop.NumReduceTasks", Integer.toString( job.getNumReduceTasks() ) );
      String taskId = job.get( "mapred.task.id" );
      variableSpace.setVariable( "Internal.Hadoop.TaskId", taskId );
      // TODO: Verify if the string range holds true for all Hadoop distributions
      // Extract the node number from the task ID. 
      // The consensus currently is that it's the part after the last underscore. 
      // 
      // Examples: 
      // job_201208090841_9999 
      // job_201208090841_10000 
      // 
      String nodeNumber;
      if ( Const.isEmpty( taskId ) ) {
        nodeNumber = "0";
      } else {
        int lastUnderscoreIndex = taskId.lastIndexOf( "_" );
        if ( lastUnderscoreIndex >= 0 ) {
          nodeNumber = taskId.substring( lastUnderscoreIndex + 1 );
        } else {
          nodeNumber = "0";
        }
      }
      // get rid of zeroes. 
      // 
      variableSpace.setVariable( "Internal.Hadoop.NodeNumber", Integer.toString( Integer.valueOf( nodeNumber ) ) );
    }
  }

  /**
   * @return the current working directory for this JVM.
   */
  public static String getWorkingDir() {
    return System.getProperty( "user.dir" );
  }

  /**
   * Determines the Kettle Home property to use for this invocation from the configuration provided. If it is not set
   * the current working directory will be used.
   *
   * @param conf Configuration to check for Kettle Home to be set in.
   * @return The Kettle Home directory to use
   */
  public static String getKettleHomeProperty( Configuration conf ) {
    String kettleHome = conf.get( PROPERTY_PENTAHO_KETTLE_HOME );
    if ( StringUtils.isEmpty( kettleHome ) ) {
      return getWorkingDir();
    }
    return kettleHome;
  }

  /**
   * Builds a comma-separated list of paths to load Kettle plugins from. To be used as the value for the System property
   * {@link Const.PLUGIN_BASE_FOLDERS_PROP}.
   *
   * @param conf Configuration to retrieve properties from
   * @return Comma-separated list of paths to look for Kettle plugins in
   */
  public static String getPluginDirProperty( final Configuration conf ) throws KettleException {
    // Load plugins from the directory specified in the configuration
    String kettlePluginDir = conf.get( PROPERTY_PENTAHO_KETTLE_PLUGINS_DIR );

    if ( StringUtils.isEmpty( kettlePluginDir ) ) {
      kettlePluginDir = getWorkingDir();
    }

    kettlePluginDir = Const.DEFAULT_PLUGIN_BASE_FOLDERS + "," + kettlePluginDir;
    return kettlePluginDir;
  }

  /**
   * Create a copy of {@code trans}
   */
  public static Trans recreateTrans( Trans trans ) {
    return new Trans( trans.getTransMeta(), trans.getParent() );
  }

  public static String getStackTrace( Throwable t ) {
    StringWriter stringWritter = new StringWriter();
    PrintWriter printWritter = new PrintWriter( stringWritter, true );
    t.printStackTrace( printWritter );
    printWritter.flush();
    stringWritter.flush();
    return stringWritter.toString();
  }

  public static void logMessage( String message ) {
    logMessage( Thread.currentThread().hashCode(), message );
  }

  public static void logMessage( Throwable t ) {
    logMessage( Thread.currentThread().hashCode(), getStackTrace( t ) );
  }

  public static void logMessage( String message, Throwable t ) {
    logMessage( Thread.currentThread().hashCode(), message );
    logMessage( Thread.currentThread().hashCode(), getStackTrace( t ) );
  }

  public static void logMessage( int id, String message ) {
    logMessage( new Integer( id ).toString(), message );
  }

  public static void logMessage( int id, Throwable t ) {
    logMessage( new Integer( id ).toString(), getStackTrace( t ) );
  }

  public static void logMessage( int id, String message, Throwable t ) {
    logMessage( new Integer( id ).toString(), message );
    logMessage( new Integer( id ).toString(), getStackTrace( t ) );
  }

  public static void logMessage( String id, String message ) {
    try {
      FileOutputStream fos = new FileOutputStream( "/tmp/PDIMapReduce.log", true ); //$NON-NLS-1$
      if ( id != null ) {
        fos.write( ( id + ": " ).getBytes() ); //$NON-NLS-1$
      }
      fos.write( message.getBytes() );
      fos.write( System.getProperty( "line.separator" ).getBytes() ); //$NON-NLS-1$
      fos.close();
    } catch ( Throwable t ) {
      //ignore
    }
  }
}
