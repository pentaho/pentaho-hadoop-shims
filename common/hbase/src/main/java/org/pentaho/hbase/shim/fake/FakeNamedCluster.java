package org.pentaho.hbase.shim.fake;

import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

import java.util.Map;

public class FakeNamedCluster implements NamedCluster {
  private String name;

  public FakeNamedCluster( String name ) {
    this.name = name;
  }

  @Override public String getName() {
    return name;
  }

  @Override public void setName( String name ) {
    this.name = name;
  }

  @Override public String getShimIdentifier() {
    return null;
  }

  @Override public void setShimIdentifier( String shimIdentifier ) {

  }

  @Override public String getStorageScheme() {
    return null;
  }

  @Override public void setStorageScheme( String storageScheme ) {

  }

  @Override public void replaceMeta( NamedCluster nc ) {

  }

  @Override public String getHdfsHost() {
    return null;
  }

  @Override public void setHdfsHost( String hdfsHost ) {

  }

  @Override public String getHdfsPort() {
    return null;
  }

  @Override public void setHdfsPort( String hdfsPort ) {

  }

  @Override public String getHdfsUsername() {
    return null;
  }

  @Override public void setHdfsUsername( String hdfsUsername ) {

  }

  @Override public String getHdfsPassword() {
    return null;
  }

  @Override public void setHdfsPassword( String hdfsPassword ) {

  }

  @Override public String getJobTrackerHost() {
    return null;
  }

  @Override public void setJobTrackerHost( String jobTrackerHost ) {

  }

  @Override public String getJobTrackerPort() {
    return null;
  }

  @Override public void setJobTrackerPort( String jobTrackerPort ) {

  }

  @Override public String getZooKeeperHost() {
    return null;
  }

  @Override public void setZooKeeperHost( String zooKeeperHost ) {

  }

  @Override public String getZooKeeperPort() {
    return null;
  }

  @Override public void setZooKeeperPort( String zooKeeperPort ) {

  }

  @Override public String getOozieUrl() {
    return null;
  }

  @Override public void setOozieUrl( String oozieUrl ) {

  }

  @Override public long getLastModifiedDate() {
    return 0;
  }

  @Override public void setLastModifiedDate( long lastModifiedDate ) {

  }

  @Override public boolean isMapr() {
    return false;
  }

  @Override public void setMapr( boolean mapr ) {

  }

  @Override public String getGatewayUrl() {
    return null;
  }

  @Override public void setGatewayUrl( String gatewayUrl ) {

  }

  @Override public String getGatewayUsername() {
    return null;
  }

  @Override public void setGatewayUsername( String gatewayUsername ) {

  }

  @Override public String getGatewayPassword() {
    return null;
  }

  @Override public void setGatewayPassword( String gatewayPassword ) {

  }

  @Override public String getKafkaBootstrapServers() {
    return null;
  }

  @Override public void setKafkaBootstrapServers( String kafkaBootstrapServers ) {

  }

  @Override public void setUseGateway( boolean selection ) {

  }

  @Override public boolean isUseGateway() {
    return false;
  }

  @Override public NamedCluster clone() {
    return null;
  }

  @Override public String toXmlForEmbed( String rootTag ) {
    return null;
  }

  @Override public NamedCluster fromXmlForEmbed( Node node ) {
    return null;
  }

  @Override
  public String processURLsubstitution( String incomingURL, IMetaStore metastore, VariableSpace variableSpace ) {
    return null;
  }

  @Override public void initializeVariablesFrom( VariableSpace variableSpace ) {

  }

  @Override public void copyVariablesFrom( VariableSpace variableSpace ) {

  }

  @Override public void shareVariablesWith( VariableSpace variableSpace ) {

  }

  @Override public VariableSpace getParentVariableSpace() {
    return null;
  }

  @Override public void setParentVariableSpace( VariableSpace variableSpace ) {

  }

  @Override public void setVariable( String s, String s1 ) {

  }

  @Override public String getVariable( String s, String s1 ) {
    return null;
  }

  @Override public String getVariable( String s ) {
    return null;
  }

  @Override public boolean getBooleanValueOfVariable( String s, boolean b ) {
    return false;
  }

  @Override public String[] listVariables() {
    return new String[ 0 ];
  }

  @Override public String environmentSubstitute( String s ) {
    return null;
  }

  @Override public String[] environmentSubstitute( String[] strings ) {
    return new String[ 0 ];
  }

  @Override public void injectVariables( Map<String, String> map ) {

  }

  @Override public String fieldSubstitute( String s, RowMetaInterface rowMetaInterface, Object[] objects )
    throws KettleValueException {
    return null;
  }
}
