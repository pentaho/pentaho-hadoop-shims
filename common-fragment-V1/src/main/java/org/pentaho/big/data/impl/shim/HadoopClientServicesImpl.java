package org.pentaho.big.data.impl.shim;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.pentaho.big.data.bundles.impl.shim.hbase.ByteConversionUtilImpl;
import com.pentaho.big.data.bundles.impl.shim.hbase.HBaseConnectionImpl;
import com.pentaho.big.data.bundles.impl.shim.hbase.ResultFactoryImpl;
import com.pentaho.big.data.bundles.impl.shim.hbase.mapping.ColumnFilterFactoryImpl;
import com.pentaho.big.data.bundles.impl.shim.hbase.mapping.MappingFactoryImpl;
import com.pentaho.big.data.bundles.impl.shim.hbase.meta.HBaseValueMetaInterfaceFactoryImpl;
import com.pentaho.big.data.bundles.impl.shim.hdfs.HadoopFileSystemImpl;
import org.apache.avro.Conversion;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.commons.io.FileUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.util.PropertiesUtil;
import org.apache.pig.tools.grunt.GruntParser;
import org.apache.pig.tools.parameters.ParameterSubstitutionPreprocessor;
import org.apache.sqoop.Sqoop;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.osgi.framework.Filter;
import org.osgi.framework.ServiceReference;
import org.osgi.framework.wiring.BundleWire;
import org.osgi.framework.wiring.BundleWiring;
import org.pentaho.big.data.impl.shim.oozie.OozieJobInfoDelegate;
import org.pentaho.big.data.impl.shim.oozie.OozieJobInfoImpl;
import org.pentaho.big.data.impl.shim.pig.PigResultImpl;
import org.pentaho.big.data.impl.shim.pig.WriterAppenderManager;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.hadoop.shim.api.HadoopClientServices;
import org.pentaho.hadoop.shim.api.HadoopClientServicesException;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.hbase.ByteConversionUtil;
import org.pentaho.hadoop.shim.api.hbase.HBaseConnection;
import org.pentaho.hadoop.shim.api.hbase.ResultFactory;
import org.pentaho.hadoop.shim.api.hdfs.HadoopFileSystem;
import org.pentaho.hadoop.shim.api.internal.Configuration;
import org.pentaho.hadoop.shim.api.internal.hbase.HBaseBytesUtilShim;
import org.pentaho.hadoop.shim.api.oozie.OozieJobInfo;
import org.pentaho.hadoop.shim.api.pig.PigResult;
import org.pentaho.hadoop.shim.common.ShimUtils;
import org.pentaho.hadoop.shim.spi.HadoopShim;
import org.pentaho.hbase.shim.common.CommonHBaseBytesUtil;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Reader;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static org.apache.oozie.client.OozieClient.APP_PATH;
import static org.apache.oozie.client.OozieClient.BUNDLE_APP_PATH;
import static org.apache.oozie.client.OozieClient.COORDINATOR_APP_PATH;

public class HadoopClientServicesImpl implements HadoopClientServices {
  private static final String[] EMPTY_STRING_ARRAY = new String[ 0 ];
  private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger( HadoopClientServicesImpl.class );
  public static final String SQOOP_THROW_ON_ERROR = "sqoop.throwOnError";
  private static final String ALT_CLASSPATH = "hadoop.alt.classpath";
  private static final String TMPJARS = "tmpjars";

  protected NamedCluster namedCluster;
  protected final OozieClient oozieClient;
  protected final HadoopShim hadoopShim;
  private BundleContext bundleContext;
  private List<String> sqoopBundleFileLocations = new ArrayList<>();
  private final WriterAppenderManager.Factory writerAppenderManagerFactory;
  protected final HBaseBytesUtilShim bytesUtil;

  private enum ExternalPigJars {

    PIG( "pig" ),
    AUTOMATON( "automaton" ),
    ANTLR( "antlr-runtime" ),
    GUAVA( "guava" ),
    JACKSON_CORE( "jackson-core-asl" ),
    JACKSON_MAPPER( "jackson-mapper-asl" ),
    JODATIME( "joda-time" );

    private final String jarName;

    ExternalPigJars( String jarName ) {
      this.jarName = jarName;
    }

    public String getJarName() {
      return jarName;
    }

  }

  public HadoopClientServicesImpl( NamedCluster namedCluster, HadoopShim hadoopShim, BundleContext bundleContext ) {
    this.bundleContext = bundleContext;
    this.hadoopShim = hadoopShim;
    this.namedCluster = namedCluster;
    this.oozieClient = new OozieClient( namedCluster.getOozieUrl() );
    this.writerAppenderManagerFactory = new WriterAppenderManager.Factory();
    this.bytesUtil = new CommonHBaseBytesUtil();
  }

  public String getOozieProtocolUrl() throws HadoopClientServicesException {
    try {
      return oozieClient.getProtocolUrl();
    } catch ( OozieClientException e ) {
      throw new HadoopClientServicesException( e, e.getErrorCode() );
    }
  }

  public OozieJobInfo runOozie( Properties props ) throws HadoopClientServicesException {
    try {
      String jobId = oozieClient.run( props );
      return new OozieJobInfoDelegate( new OozieJobInfoImpl( jobId, oozieClient ) );
    } catch ( OozieClientException e ) {
      throw new HadoopClientServicesException( e, e.getErrorCode() );
    }
  }

  public void validateOozieWSVersion() throws HadoopClientServicesException {
    try {
      oozieClient.validateWSVersion();
    } catch ( OozieClientException e ) {
      throw new HadoopClientServicesException( e, e.getErrorCode() );
    }
  }

  public boolean hasOozieAppPath( Properties props ) {
    return props.containsKey( APP_PATH )
      || props.containsKey( COORDINATOR_APP_PATH )
      || props.containsKey( BUNDLE_APP_PATH );
  }

  public int runSqoop( List<String> argsList, Properties properties ) {
    Configuration configuration = hadoopShim.createConfiguration( namedCluster.getName() );
    for ( Map.Entry<String, String> entry : Maps.fromProperties( properties ).entrySet() ) {
      configuration.set( entry.getKey(), entry.getValue() );
    }

    try {
      // Make sure Sqoop throws exceptions instead of returning a status of 1
      System.setProperty( SQOOP_THROW_ON_ERROR, Boolean.toString( true ) );
      configureShim( configuration );
      String[] args = argsList.toArray( new String[ argsList.size() ] );
      Configuration c = configuration;
      ClassLoader cl = Thread.currentThread().getContextClassLoader();
      Thread.currentThread().setContextClassLoader( getClass().getClassLoader() );
      String tmpPropertyHolder = System.getProperty( ALT_CLASSPATH );
      try {
        loadBundleFilesLocations();
        System.setProperty( ALT_CLASSPATH, createHadoopAltClasspath() );
        c.set( TMPJARS, getSqoopJarLocation( c ) );
        if ( args.length > 0
          && ( Arrays.asList( args ).contains( "--as-avrodatafile" )
            || Arrays.asList( args ).contains( "--export-dir" ) ) ) { // BACKLOG-32217: Avro libs needed for export
          addDependencyJars( c, Conversion.class, AvroWrapper.class );
        }
        if ( args.length > 0 && Arrays.asList( args ).contains( "--hbase-table" ) ) {
          Filter serviceFilter = bundleContext.createFilter( "(shim=" + namedCluster.getShimIdentifier() + ")" );
          ServiceReference serviceReference =
            (ServiceReference) bundleContext.getServiceReferences( HadoopShim.class, serviceFilter.toString() )
              .toArray()[ 0 ];
          Object service = bundleContext.getService( serviceReference );
          Class[] depClasses = (Class[]) service.getClass().getMethod( "getHbaseDependencyClasses" ).invoke( service );
          addDependencyJars( c, depClasses );
        }
        return Sqoop.runTool( args, ShimUtils.asConfiguration( c ) );
      } catch ( IOException e ) {
        e.printStackTrace();
        return -1;
      } finally {
        Thread.currentThread().setContextClassLoader( cl );
        if ( tmpPropertyHolder == null ) {
          System.clearProperty( ALT_CLASSPATH );
        } else {
          System.setProperty( ALT_CLASSPATH, tmpPropertyHolder );
        }
      }
    } catch ( Exception e ) {
      LOGGER.error( "Error executing sqoop", e );
      return 1;
    }
  }

  private void configureShim( Configuration conf ) throws Exception {
    List<String> messages = Lists.newArrayList();

    if ( namedCluster.isMapr() ) {
      hadoopShim.configureConnectionInformation( "", "", "", "", conf, messages );
    } else {
      hadoopShim.configureConnectionInformation(
        namedCluster.environmentSubstitute( namedCluster.getHdfsHost() ),
        namedCluster.environmentSubstitute( namedCluster.getHdfsPort() ),
        namedCluster.environmentSubstitute( namedCluster.getJobTrackerHost() ),
        namedCluster.environmentSubstitute( namedCluster.getJobTrackerPort() ), conf, messages );
    }

    for ( String m : messages ) {
      LOGGER.info( m );
    }
  }

  private String getSqoopJarLocation( Configuration c ) {

    StringBuilder sb = new StringBuilder();

    for ( String bundleFileLocation : sqoopBundleFileLocations ) {
      File filesInsideBundle = new File( bundleFileLocation );
      Iterator<File> filesIterator = FileUtils.iterateFiles( filesInsideBundle, new String[] { "jar" }, true );

      while ( filesIterator.hasNext() ) {
        File file = filesIterator.next();
        String name = file.getName();
        if ( name.startsWith( "sqoop" ) ) {
          sb.append( file.getAbsolutePath() );
        }
      }
    }

    try {
      FileSystem fs = FileSystem.getLocal( ShimUtils.asConfiguration( c ) );
      return new Path( sb.toString() ).makeQualified( fs ).toString();
    } catch ( IOException e ) {
      e.printStackTrace();
    }
    return sb.toString();
  }

  private void addDependencyJars( Configuration conf, Class... classes )
    throws IOException {
    List<String> classNames = new ArrayList<>();
    for ( Class clazz : classes ) {
      classNames.add( clazz.getCanonicalName().replace( ".", "/" ) + ".class" );
    }
    Set<String> tmpjars = new HashSet<>();
    if ( conf.get( TMPJARS ) != null ) {
      tmpjars.addAll( Arrays.asList( conf.get( TMPJARS ).split( "," ) ) );
    }

    for ( String bundleFileLocation : sqoopBundleFileLocations ) {
      File filesInsideBundle = new File( bundleFileLocation );
      Iterator<File> filesIterator = FileUtils.iterateFiles( filesInsideBundle, new String[] { "jar" }, true );

      getOut:
      while ( filesIterator.hasNext() ) {
        File file = filesIterator.next();

        // Process the jar file.

        try ( ZipFile zip = new ZipFile( file ) ) {
          // Loop through the jar entries and print the name of each one.

          for ( Enumeration list = zip.entries(); list.hasMoreElements(); ) {
            ZipEntry entry = (ZipEntry) list.nextElement();
            if ( !entry.isDirectory() && entry.getName().endsWith( ".class" ) ) {
              ListIterator<String> classNameIterator = classNames.listIterator();
              while ( classNameIterator.hasNext() ) {
                if ( entry.getName().endsWith( classNameIterator.next() ) ) {
                  // If here we found a class in this jar, add the jar to the list, and delete the class from
                  // classNames.
                  tmpjars.add( file.toURI().toURL().toString() );
                  classNameIterator.remove();
                  if ( classNames.size() == 0 ) {
                    break getOut;
                  }
                }
              }
            }
          }
        }
      }
    }

    StringBuilder sb = new StringBuilder();
    if ( tmpjars.size() > 0 ) {
      for ( String jarPath : tmpjars ) {
        sb.append( "," ).append( jarPath );
      }
      conf.set( TMPJARS, sb.toString().substring( 1 ) );
    }
  }

  private void loadBundleFilesLocations() {
    sqoopBundleFileLocations.clear();
    String bundleLocation = bundleContext.getBundle().getDataFile( "" ).getParent();
    sqoopBundleFileLocations.add( bundleLocation );
    BundleWiring wiring = bundleContext.getBundle().adapt( BundleWiring.class );
    List<BundleWire> fragments = wiring.getProvidedWires( "osgi.wiring.host" );
    for ( BundleWire fragment : fragments ) {
      Bundle fragmentBundle = fragment.getRequirerWiring().getBundle();
      String fragmentBundleLocation = fragmentBundle.getDataFile( "" ).getParent();
      sqoopBundleFileLocations.add( fragmentBundleLocation );
    }
  }

  private String createHadoopAltClasspath() {

    StringBuilder sb = new StringBuilder();

    for ( String bundleFileLocation : sqoopBundleFileLocations ) {
      File filesInsideBundle = new File( bundleFileLocation );
      Iterator<File> filesIterator = FileUtils.iterateFiles( filesInsideBundle, new String[] { "jar" }, true );

      while ( filesIterator.hasNext() ) {
        File file = filesIterator.next();
        String name = file.getName();
        if ( name.startsWith( "hadoop-common" )
          || name.startsWith( "hadoop-mapreduce-client-core" )
          || name.startsWith( "hadoop-core" )
          || name.startsWith( "sqoop" ) ) {
          sb.append( file.getAbsolutePath() );
          sb.append( File.pathSeparator );
        }
      }
    }

    return sb.toString();
  }

  public PigResult runPig( String scriptPath, PigExecutionMode executionMode, List<String> parameters, String name,
                           LogChannelInterface logChannelInterface, VariableSpace variableSpace,
                           LogLevel logLevel ) {
    FileObject appenderFile = null;
    try ( WriterAppenderManager appenderManager = writerAppenderManagerFactory.create( logChannelInterface, logLevel,
      name ) ) {
      appenderFile = appenderManager.getFile();
      Configuration configuration = hadoopShim.createConfiguration( namedCluster.getName() );
      if ( executionMode != PigExecutionMode.LOCAL ) {
        List<String> configMessages = new ArrayList<String>();
        hadoopShim.configureConnectionInformation( variableSpace.environmentSubstitute( namedCluster.getHdfsHost() ),
          variableSpace.environmentSubstitute( namedCluster.getHdfsPort() ),
          variableSpace.environmentSubstitute( namedCluster.getJobTrackerHost() ),
          variableSpace.environmentSubstitute( namedCluster.getJobTrackerPort() ), configuration,
          configMessages );
        if ( logChannelInterface != null ) {
          for ( String configMessage : configMessages ) {
            logChannelInterface.logBasic( configMessage );
          }
        }
      }
      URL scriptU;
      String scriptFileS = scriptPath;
      scriptFileS = variableSpace.environmentSubstitute( scriptFileS );
      if ( scriptFileS.indexOf( "://" ) == -1 ) {
        File scriptFile = new File( scriptFileS );
        scriptU = scriptFile.toURI().toURL();
      } else {
        scriptU = new URL( scriptFileS );
      }
      String pigScript = substitutePigScriptParameters( scriptU, parameters );
      Properties properties = new Properties();
      updatePigConfiguration( properties, executionMode == PigExecutionMode.LOCAL ? null : configuration );
      return new PigResultImpl( appenderFile, executePigScript( pigScript,
        executionMode == PigExecutionMode.LOCAL ? PigExecutionMode.LOCAL : PigExecutionMode.MAPREDUCE, properties ),
        null );
    } catch ( Exception e ) {
      return new PigResultImpl( appenderFile, null, e );
    }
  }

  private void updatePigConfiguration( Properties properties, Configuration configuration ) {
    PropertiesUtil.loadDefaultProperties( properties );
    if ( configuration != null ) {
      properties.putAll( ConfigurationUtil.toProperties( ShimUtils.asConfiguration( configuration ) ) );
      properties.setProperty( "pig.use.overriden.hadoop.configs", "true" );
    }
  }

  private void addExternalJarsToPigContext( PigContext pigContext ) throws MalformedURLException {
    File filesInsideBundle = new File( bundleContext.getBundle().getDataFile( "" ).getParent() );
    Iterator<File> filesIterator = FileUtils.iterateFiles( filesInsideBundle, new String[] { "jar" }, true );
    while ( filesIterator.hasNext() ) {
      File file = filesIterator.next();
      addMatchedJarToPigContext( pigContext, file );
    }
  }

  private void addMatchedJarToPigContext( PigContext pigContext, File jarFile ) throws MalformedURLException {
    String jarName = jarFile.getName();
    for ( ExternalPigJars externalPigJars : ExternalPigJars.values() ) {
      if ( jarName.startsWith( externalPigJars.getJarName() ) ) {
        String jarPath = jarFile.getAbsolutePath();
        pigContext.addJar( jarPath );
        break;
      }
    }
  }

  private int[] executePigScript( String pigScript, PigExecutionMode mode, Properties properties )
    throws IOException, org.apache.pig.tools.pigscript.parser.ParseException {
    GruntParser grunt = null;
    PigContext pigContext = new PigContext( getExecType( mode ), properties );
    addExternalJarsToPigContext( pigContext );
    PigServer pigServer = new PigServer( pigContext );
    try {
      Constructor constructor = GruntParser.class.getConstructor( Reader.class, PigServer.class );
      grunt = (GruntParser) constructor.newInstance( new StringReader( pigScript ), pigServer );
    } catch ( Exception e ) {
      try {
        Constructor constructor = GruntParser.class.getConstructor( Reader.class );
        grunt = (GruntParser) constructor.newInstance( new StringReader( pigScript ) );
        Method method = grunt.getClass().getMethod( "setParams", new Class[] { PigServer.class } );
        method.invoke( grunt, pigServer );
      } catch ( Exception e1 ) {
        throw new org.apache.pig.tools.pigscript.parser.ParseException(
          "Error constructing Grunt Parser in " + getClass().getName() );
      }
    }
    if ( grunt == null ) {
      throw new org.apache.pig.tools.pigscript.parser.ParseException(
        "Grunt Parser is null in " + getClass().getName() );
    }
    grunt.setInteractive( false );
    int[] retValues = grunt.parseStopOnError( false );
    return retValues;
  }

  protected ExecType getExecType( PigExecutionMode mode ) {
    switch ( mode ) {
      case LOCAL:
        return ExecType.LOCAL;
      case MAPREDUCE:
        return ExecType.MAPREDUCE;
      default:
        throw new IllegalStateException( "unknown execution mode: " + mode );
    }
  }

  private String substitutePigScriptParameters( URL pigScript, List<String> paramList ) throws Exception {
    final InputStream inStream = pigScript.openStream();
    StringWriter writer = new StringWriter();
    // do parameter substitution
    ParameterSubstitutionPreprocessor psp = new ParameterSubstitutionPreprocessor( 50 );
    psp.genSubstitutedFile( new BufferedReader( new InputStreamReader( inStream ) ),
      writer,
      paramList.size() > 0 ? paramList.toArray( EMPTY_STRING_ARRAY ) : null, null );
    return writer.toString();
  }

  public NamedCluster getNamedCluster() {
    return namedCluster;
  }

  public HadoopFileSystem getFileSystem( NamedCluster namedCluster, URI uri ) throws IOException {
    final Configuration configuration = hadoopShim.createConfiguration( namedCluster.getName() );
    FileSystem fileSystem = (FileSystem) hadoopShim.getFileSystem( configuration ).getDelegate();
    if ( fileSystem instanceof LocalFileSystem ) {
      LOGGER.error( "Got a local filesystem, was expecting an hdfs connection" );
      throw new IOException( "Got a local filesystem, was expecting an hdfs connection" );
    }

    final URI finalUri = fileSystem.getUri() != null ? fileSystem.getUri() : uri;
    HadoopFileSystem hadoopFileSystem = new HadoopFileSystemImpl( () -> {
      try {
        return finalUri != null
          ? (FileSystem) hadoopShim.getFileSystem( finalUri, configuration, (NamedCluster) namedCluster ).getDelegate()
          : (FileSystem) hadoopShim.getFileSystem( configuration ).getDelegate();
      } catch ( IOException | InterruptedException e ) {
        LOGGER.debug( "Error looking up/creating the file system ", e );
        return null;
      }
    } );
    ( (HadoopFileSystemImpl) hadoopFileSystem ).setNamedCluster( namedCluster );

    return hadoopFileSystem;
  }


  protected HBaseConnectionImpl getConnectionImpl( Properties connProps, LogChannelInterface logChannelInterface )
    throws IOException {
    return new HBaseConnectionImpl( null, bytesUtil, connProps, logChannelInterface );
  }

  public HBaseConnection getHBaseConnection( VariableSpace variableSpace, String siteConfig, String defaultConfig,
                                             LogChannelInterface logChannelInterface ) throws IOException {
    Properties connProps = new Properties();
    String zooKeeperHost = null;
    String zooKeeperPort = null;
    if ( namedCluster != null ) {
      zooKeeperHost = variableSpace.environmentSubstitute( namedCluster.getZooKeeperHost() );
      zooKeeperPort = variableSpace.environmentSubstitute( namedCluster.getZooKeeperPort() );
    }
    if ( !Const.isEmpty( zooKeeperHost ) ) {
      connProps.setProperty( org.pentaho.hadoop.shim.spi.HBaseConnection.ZOOKEEPER_QUORUM_KEY, zooKeeperHost );
    }
    if ( !Const.isEmpty( zooKeeperPort ) ) {
      connProps.setProperty( org.pentaho.hadoop.shim.spi.HBaseConnection.ZOOKEEPER_PORT_KEY, zooKeeperPort );
    }
    if ( !Const.isEmpty( siteConfig ) ) {
      connProps.setProperty( org.pentaho.hadoop.shim.spi.HBaseConnection.SITE_KEY, siteConfig );
    }
    if ( !Const.isEmpty( defaultConfig ) ) {
      connProps.setProperty( org.pentaho.hadoop.shim.spi.HBaseConnection.DEFAULTS_KEY, defaultConfig );
    }
    connProps.setProperty( "named.cluster", namedCluster.getName() );
    return getConnectionImpl( connProps, logChannelInterface );
  }

  public ColumnFilterFactoryImpl getHBaseColumnFilterFactory() {
    return new ColumnFilterFactoryImpl();
  }

  public MappingFactoryImpl getHBaseMappingFactory() {
    return new MappingFactoryImpl( bytesUtil, getHBaseValueMetaInterfaceFactory() );
  }

  public HBaseValueMetaInterfaceFactoryImpl getHBaseValueMetaInterfaceFactory() {
    return new HBaseValueMetaInterfaceFactoryImpl( bytesUtil );
  }

  public ByteConversionUtil getHBaseByteConversionUtil() {
    return (ByteConversionUtil) new ByteConversionUtilImpl( bytesUtil );
  }

  public ResultFactory getHBaseResultFactory() {
    return new ResultFactoryImpl( bytesUtil );
  }
}
