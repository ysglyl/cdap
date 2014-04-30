package com.continuuity.security.runtime;

import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.guice.IOModule;
import com.continuuity.common.guice.ZKClientModule;
import com.continuuity.common.runtime.DaemonMain;
import com.continuuity.security.guice.SecurityModules;
import com.continuuity.security.server.ExternalAuthenticationServer;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.twill.common.Services;
import org.apache.twill.zookeeper.ZKClientService;

/**
 *
 */
public class AuthenticationServerMain extends DaemonMain {
  private ZKClientService zkClientService;
  private ExternalAuthenticationServer authServer;

  @Override
  public void init(String[] args) {
    Injector injector = Guice.createInjector(new ConfigModule(),
                                             new IOModule(),
                                             new SecurityModules().getDistributedModules(),
                                             new DiscoveryRuntimeModule().getDistributedModules(),
                                             new ZKClientModule());
    this.zkClientService = injector.getInstance(ZKClientService.class);
    this.authServer = injector.getInstance(ExternalAuthenticationServer.class);
  }

  @Override
  public void start() {
    Services.chainStart(zkClientService, authServer);
  }

  @Override
  public void stop() {
    authServer.stop();
  }

  @Override
  public void destroy() {
  }

  public static void main(String[] args) throws Exception {
    new AuthenticationServerMain().doMain(args);
  }
}
