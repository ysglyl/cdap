package com.continuuity.common.utils;

import org.apache.commons.lang.StringUtils;

import java.io.PrintStream;

public class Copyright {

  private static final String[] lines = {
      StringUtils.repeat("=", 80),
      " Continuuity BigFlow - Copyright 2012 Continuuity, Inc. All Rights Reserved.",
      ""
  };

  public static void print(PrintStream out) {
    for (String line : lines) out.println(line);
  }

  public static void print() {
    print(System.out);
  }
}
