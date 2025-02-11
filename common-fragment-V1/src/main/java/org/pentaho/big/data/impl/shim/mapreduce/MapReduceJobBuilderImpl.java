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


package org.pentaho.big.data.impl.shim.mapreduce;

import org.pentaho.hadoop.shim.api.cluster.NamedCluster;

import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.hadoop.shim.api.internal.Configuration;
import org.pentaho.hadoop.shim.api.internal.fs.FileSystem;
import org.pentaho.hadoop.shim.api.internal.fs.Path;
import org.pentaho.hadoop.shim.api.mapreduce.MapReduceJobAdvanced;
import org.pentaho.hadoop.shim.api.mapreduce.MapReduceJobBuilder;
import org.pentaho.hadoop.shim.spi.HadoopShim;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by bryan on 12/3/15.
 */
public class MapReduceJobBuilderImpl implements MapReduceJobBuilder {
  private final NamedCluster namedCluster;
  private final HadoopShim hadoopShim;
  private final LogChannelInterface log;
  private final VariableSpace variableSpace;
  private final Map<String, String> userDefined;
  private URL resolvedJarUrl;
  private String jarUrl;
  private String hadoopJobName;
  private String outputKeyClass;
  private String outputValueClass;
  private String mapperClass;
  private String combinerClass;
  private String reducerClass;
  private String inputFormatClass;
  private String outputFormatClass;
  private String[] inputPaths;
  private int numMapTasks;
  private int numReduceTasks;
  private String outputPath;
  private String mapOutputKeyClass;
  private String mapOutputValueClass;
  private String mapRunnerClass;

  public MapReduceJobBuilderImpl( NamedCluster namedCluster, HadoopShim hadoopShim, LogChannelInterface log,
                                  VariableSpace variableSpace ) {
    this.namedCluster = namedCluster;
    this.hadoopShim = hadoopShim;
    this.log = log;
    this.variableSpace = variableSpace;
    this.userDefined = new HashMap<>();
  }

  @Override
  public void setResolvedJarUrl( URL resolvedJarUrl ) {
    this.resolvedJarUrl = resolvedJarUrl;
  }

  @Override
  public void setJarUrl( String jarUrl ) {
    this.jarUrl = jarUrl;
  }

  @Override
  public void setHadoopJobName( String hadoopJobName ) {
    this.hadoopJobName = hadoopJobName;
  }

  @Override
  public void setOutputKeyClass( String outputKeyClass ) {
    this.outputKeyClass = outputKeyClass;
  }

  @Override
  public void setMapOutputKeyClass( String mapOutputKeyClass ) {
    this.mapOutputKeyClass = mapOutputKeyClass;
  }

  @Override
  public void setMapOutputValueClass( String mapOutputValueClass ) {
    this.mapOutputValueClass = mapOutputValueClass;
  }

  @Override
  public void setMapRunnerClass( String mapRunnerClass ) {
    this.mapRunnerClass = mapRunnerClass;
  }

  @Override
  public void setOutputValueClass( String outputValueClass ) {
    this.outputValueClass = outputValueClass;
  }

  @Override
  public void setMapperClass( String mapperClass ) {
    this.mapperClass = mapperClass;
  }

  @Override
  public void setCombinerClass( String combinerClass ) {
    this.combinerClass = combinerClass;
  }

  @Override
  public void setReducerClass( String reducerClass ) {
    this.reducerClass = reducerClass;
  }

  @Override
  public void setInputFormatClass( String inputFormatClass ) {
    this.inputFormatClass = inputFormatClass;
  }

  @Override
  public void setOutputFormatClass( String outputFormatClass ) {
    this.outputFormatClass = outputFormatClass;
  }

  @Override
  public void setInputPaths( String[] inputPaths ) {
    this.inputPaths = inputPaths;
  }

  @Override
  public void setNumMapTasks( int numMapTasks ) {
    this.numMapTasks = numMapTasks;
  }

  @Override
  public void setNumReduceTasks( int numReduceTasks ) {
    this.numReduceTasks = numReduceTasks;
  }

  @Override
  public void setOutputPath( String outputPath ) {
    this.outputPath = outputPath;
  }

  @Override
  public void set( String key, String value ) {
    userDefined.put( key, value );
  }

  protected void configure( Configuration conf ) throws Exception {
    FileSystem fs = hadoopShim.getFileSystem( conf );
    URL[] urls = resolvedJarUrl == null ? new URL[] {} : new URL[] { resolvedJarUrl };
    try ( URLClassLoader loader = new URLClassLoader( urls, hadoopShim.getClass().getClassLoader() ) ) {
      conf.setJobName( hadoopJobName );

      if ( outputKeyClass != null ) {
        Class<?> keyClass = loader.loadClass( outputKeyClass );
        conf.setOutputKeyClass( keyClass );
      }
      if ( outputValueClass != null ) {
        Class<?> valueClass = loader.loadClass( outputValueClass );
        conf.setOutputValueClass( valueClass );
      }
      if ( mapOutputKeyClass != null ) {
        Class<?> keyClass = loader.loadClass( mapOutputKeyClass );
        conf.setMapOutputKeyClass( keyClass );
      }
      if ( mapOutputValueClass != null ) {
        Class<?> valueClass = loader.loadClass( mapOutputValueClass );
        conf.setMapOutputValueClass( valueClass );
      }
      if ( mapRunnerClass != null ) {
        conf.setMapRunnerClass( mapRunnerClass );
      }

      if ( mapperClass != null ) {
        Class<?> mapper = loader.loadClass( mapperClass );
        conf.setMapperClass( mapper );
      }
      if ( combinerClass != null ) {
        Class<?> combiner = loader.loadClass( combinerClass );
        conf.setCombinerClass( combiner );
      }
      if ( reducerClass != null ) {
        Class<?> reducer = loader.loadClass( reducerClass );
        conf.setReducerClass( reducer );
      }

      if ( inputFormatClass != null ) {
        Class<?> inputFormat = loader.loadClass( inputFormatClass );
        conf.setInputFormat( inputFormat );
      }
      if ( outputFormatClass != null ) {
        Class<?> outputFormat = loader.loadClass( outputFormatClass );
        conf.setOutputFormat( outputFormat );
      }

      String hdfsHostnameS = variableSpace.environmentSubstitute( namedCluster.getHdfsHost() );
      String hdfsPortS = variableSpace.environmentSubstitute( namedCluster.getHdfsPort() );
      String jobTrackerHostnameS = variableSpace.environmentSubstitute( namedCluster.getJobTrackerHost() );
      String jobTrackerPortS = variableSpace.environmentSubstitute( namedCluster.getJobTrackerPort() );

      List<String> configMessages = new ArrayList<>();
      hadoopShim.configureConnectionInformation( hdfsHostnameS, hdfsPortS, jobTrackerHostnameS, jobTrackerPortS, conf,
        configMessages );
      for ( String m : configMessages ) {
        log.logBasic( m );
      }

      List<Path> paths = new ArrayList<Path>();
      for ( String path : inputPaths ) {
        paths.add( getPath( conf, fs, path ) );
      }
      Path[] finalPaths = paths.toArray( new Path[ paths.size() ] );

      conf.setInputPaths( finalPaths );
      conf.setOutputPath( getOutputPath( conf, fs ) );

      // process user defined values
      for ( Map.Entry<String, String> stringStringEntry : userDefined.entrySet() ) {
        String key = stringStringEntry.getKey();
        String value = stringStringEntry.getValue();
        if ( key != null && !"".equals( key ) && value != null && !"".equals( value ) ) {
          conf.set( key, value );
        }
      }

      if ( jarUrl != null ) {
        conf.setJar( jarUrl );
      }

      conf.setNumMapTasks( numMapTasks );
      conf.setNumReduceTasks( numReduceTasks );
    }
  }

  public VariableSpace getVariableSpace() {
    return variableSpace;
  }

  protected Path getOutputPath( Configuration conf, FileSystem fs ) {
    return getPath( conf, fs, outputPath );
  }

  private Path getPath( Configuration conf, FileSystem fs, String outputPath ) {
    return fs.asPath( conf.getDefaultFileSystemURL(), outputPath );
  }

  protected MapReduceJobAdvanced submit( Configuration conf, String shimIdentifier ) throws IOException {
    return new RunningJobMapReduceJobAdvancedImpl( hadoopShim.submitJob( conf ) );
  }

  @Override
  public final MapReduceJobAdvanced submit() throws Exception {
    Configuration conf = hadoopShim.createConfiguration( namedCluster );
    configure( conf );
    return submit( conf, namedCluster.getShimIdentifier() );
  }

  protected String getClusterName() {
    return namedCluster.getName();
  }

  protected NamedCluster getNamedCluster() {
    return namedCluster;
  }
}
