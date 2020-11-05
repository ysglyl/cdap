/*
 * Copyright © 2020 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.cdap.internal.accelerator;

import com.google.inject.Inject;
import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.internal.app.services.ApplicationLifecycleService;
import io.cdap.cdap.metadata.MetadataAdmin;
import io.cdap.cdap.proto.ApplicationDetail;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.spi.metadata.MetadataRecord;
import io.cdap.cdap.spi.metadata.SearchRequest;
import io.cdap.cdap.spi.metadata.SearchResponse;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Class with helpful methods for dynamic accelerator framework
 */
public class AcceleratorManager {

  private static final Logger LOG = LoggerFactory.getLogger(AcceleratorManager.class);
  public static final String ACCELERATOR_TAG = "accelerator:%s";
  private final MetadataAdmin metadataAdmin;
  private final ApplicationLifecycleService applicationLifecycleService;
  private static LocationFactory locationFactory;

  @Inject
  AcceleratorManager(MetadataAdmin metadataAdmin, ApplicationLifecycleService applicationLifecycleService) {
    this.metadataAdmin = metadataAdmin;
    this.applicationLifecycleService = applicationLifecycleService;
  }

  /**
   * Returns the list of applications that are having metadata tagged with the accelerator
   *
   * @param namespace
   * @param accelerator
   * @return
   * @throws Exception
   */
  public List<ApplicationDetail> getAppsForAccelerator(NamespaceId namespace,
                                                       String accelerator) throws Exception {
    String acceleratorTag = String.format(ACCELERATOR_TAG, accelerator);
    SearchResponse searchResponse = metadataAdmin
      .search(SearchRequest.of(acceleratorTag).addNamespace(namespace.getNamespace()).build());
    List<MetadataRecord> results = searchResponse.getResults();
    Set<ApplicationId> applicationIds = results.stream()
      .map(MetadataRecord::getEntity)
      .filter(this::isApplicationType)
      .map(this::getApplicationId)
      .collect(Collectors.toSet());
    return applicationLifecycleService.getAppDetails(applicationIds).values().stream().collect(Collectors.toList());
  }

  private boolean isApplicationType(MetadataEntity metadataEntity) {
    return MetadataEntity.APPLICATION.equals(metadataEntity.getType());
  }

  private ApplicationId getApplicationId(MetadataEntity metadataEntity) {
    return new ApplicationId(metadataEntity.getValue(MetadataEntity.NAMESPACE),
                             metadataEntity.getValue(MetadataEntity.APPLICATION),
                             metadataEntity.getValue(MetadataEntity.VERSION));
  }
}
