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

package org.pentaho.bigdata.api.mapreduce;

import org.pentaho.di.trans.TransConfiguration;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by ccaspanello on 8/29/2016.
 */
public class MapReduceTransformations {

  private Optional<TransConfiguration> combiner;
  private Optional<TransConfiguration> mapper;
  private Optional<TransConfiguration> reducer;

  public MapReduceTransformations() {
    this.combiner = Optional.empty();
    this.mapper = Optional.empty();
    this.reducer = Optional.empty();
  }

  //<editor-fold desc="Getters & Setters">
  public Optional<TransConfiguration> getCombiner() {
    return combiner;
  }

  public void setCombiner( Optional<TransConfiguration> combiner ) {
    this.combiner = combiner;
  }

  public Optional<TransConfiguration> getMapper() {
    return mapper;
  }

  public void setMapper( Optional<TransConfiguration> mapper ) {
    this.mapper = mapper;
  }

  public Optional<TransConfiguration> getReducer() {
    return reducer;
  }

  public void setReducer( Optional<TransConfiguration> reducer ) {
    this.reducer = reducer;
  }
  //</editor-fold>

  public List<TransConfiguration> presentTransformations() {
    return Stream.of( combiner, mapper, reducer ).filter( Optional::isPresent ).map( Optional::get )
      .collect( Collectors.toList() );
  }
}
