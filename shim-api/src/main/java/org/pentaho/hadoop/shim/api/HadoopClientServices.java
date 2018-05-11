package org.pentaho.hadoop.shim.api;

import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.hbase.HBaseConnection;
import org.pentaho.hadoop.shim.api.hdfs.HadoopFileSystem;
import org.pentaho.hadoop.shim.api.oozie.OozieJobInfo;
import org.pentaho.hadoop.shim.api.pig.PigResult;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Properties;

public interface HadoopClientServices {
  enum PigExecutionMode {
    LOCAL, MAPREDUCE
  }

  void validateOozieWSVersion() throws HadoopClientServicesException;

  boolean hasOozieAppPath( Properties props );

  String getOozieProtocolUrl() throws HadoopClientServicesException;

  OozieJobInfo runOozie( Properties props ) throws HadoopClientServicesException;

  int runSqoop( List<String> argsList, Properties properties );

  PigResult runPig( String scriptPath, PigExecutionMode executionMode, List<String> parameters, String name,
                    LogChannelInterface logChannelInterface, VariableSpace variableSpace,
                    LogLevel logLevel );

  HadoopFileSystem getFileSystem( NamedCluster namedCluster, URI uri ) throws IOException;

  HBaseConnection getHBaseConnection( VariableSpace variableSpace, String siteConfig, String defaultConfig,
                                      LogChannelInterface logChannelInterface ) throws IOException;
}

