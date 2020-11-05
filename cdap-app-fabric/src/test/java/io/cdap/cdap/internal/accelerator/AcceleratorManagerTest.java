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

import com.google.common.io.Files;
import io.cdap.cdap.AppWithWorkflow;
import io.cdap.cdap.WorkflowAppWithFork;
import io.cdap.cdap.api.annotation.Requirements;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.common.test.AppJarHelper;
import io.cdap.cdap.internal.AppFabricTestHelper;
import io.cdap.cdap.internal.app.services.ApplicationLifecycleService;
import io.cdap.cdap.internal.app.services.http.AppFabricTestBase;
import io.cdap.cdap.proto.ApplicationDetail;
import io.cdap.cdap.proto.id.NamespaceId;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.List;

public class AcceleratorManagerTest extends AppFabricTestBase {

  private static ApplicationLifecycleService applicationLifecycleService;
  private static AcceleratorManager acceleratorManager;
  private static LocationFactory locationFactory;

  @BeforeClass
  public static void setup() throws Exception {
    applicationLifecycleService = getInjector().getInstance(ApplicationLifecycleService.class);
    acceleratorManager = getInjector().getInstance(AcceleratorManager.class);
    locationFactory = getInjector().getInstance(LocationFactory.class);
  }

  @AfterClass
  public static void stop() {
    AppFabricTestHelper.shutdown();
  }

  @Test
  public void testGetAppsWithAccelerator() throws Exception {
    //Deploy application with accelerator
    Class<AppWithWorkflow> appWithWorkflowClass = AppWithWorkflow.class;
    Requirements declaredAnnotation = appWithWorkflowClass.getDeclaredAnnotation(Requirements.class);
    //verify this app has accelerators
    Assert.assertTrue(declaredAnnotation.accelerators().length > 0);
    Id.Artifact artifactIdWithAccelerator = deployApp(appWithWorkflowClass);

    //Deploy application without accelerator
    Class<WorkflowAppWithFork> appNoAcceleratorClass = WorkflowAppWithFork.class;
    Requirements declaredAnnotation1 = appNoAcceleratorClass.getDeclaredAnnotation(Requirements.class);
    //verify this app has no accelerators
    Assert.assertTrue(declaredAnnotation1 == null);
    deployApp(appNoAcceleratorClass);

    //verify that list applications return the application tagged with accelerator only
    for (String accelerator : declaredAnnotation.accelerators()) {
      List<ApplicationDetail> appsReturned = acceleratorManager
        .getAppsForAccelerator(NamespaceId.DEFAULT, accelerator);
      appsReturned.stream().forEach(
        applicationDetail -> Assert
          .assertEquals(artifactIdWithAccelerator.getName(), applicationDetail.getArtifact().getName()));
    }

    //delete the app and verify nothing is returned.
    applicationLifecycleService.removeApplication(NamespaceId.DEFAULT.app(appWithWorkflowClass.getSimpleName()));
    for (String accelerator : declaredAnnotation.accelerators()) {
      List<ApplicationDetail> appsReturned = acceleratorManager
        .getAppsForAccelerator(NamespaceId.DEFAULT, accelerator);
      Assert.assertTrue(appsReturned.isEmpty());
    }
    applicationLifecycleService.removeApplication(NamespaceId.DEFAULT.app(appNoAcceleratorClass.getSimpleName()));
  }

  private Id.Artifact deployApp(Class applicationClass) throws Exception {
    Id.Artifact artifactId = Id.Artifact
      .from(Id.Namespace.DEFAULT, applicationClass.getSimpleName(), "1.0.0-SNAPSHOT");
    Location appJar = AppJarHelper.createDeploymentJar(locationFactory, applicationClass);
    File appJarFile = new File(tmpFolder.newFolder(),
                               String.format("%s-%s.jar", artifactId.getName(), artifactId.getVersion().getVersion()));
    Files.copy(Locations.newInputSupplier(appJar), appJarFile);
    appJar.delete();
    //deploy app
    applicationLifecycleService
      .deployAppAndArtifact(NamespaceId.DEFAULT, applicationClass.getSimpleName(), artifactId, appJarFile, null,
                            null, programId -> {
        }, true);
    return artifactId;
  }
}
