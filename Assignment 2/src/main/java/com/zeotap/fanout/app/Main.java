package com.zeotap.fanout.app;

import com.zeotap.fanout.core.Engine;
import com.zeotap.fanout.core.config.AppConfig;
import com.zeotap.fanout.core.config.ConfigLoader;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

public final class Main {
  public static void main(String[] args) throws Exception {
    Map<String, String> parsed = parseArgs(args);
    String configPath = parsed.getOrDefault("--config", "./config/config.json");

    AppConfig config = ConfigLoader.load(Path.of(configPath));
    new Engine(config).run();
  }

  private static Map<String, String> parseArgs(String[] args) {
    Map<String, String> out = new HashMap<>();
    for (int i = 0; i < args.length; i++) {
      String a = args[i];
      if (a.startsWith("--")) {
        if (i + 1 < args.length && !args[i + 1].startsWith("--")) {
          out.put(a, args[i + 1]);
          i++;
        } else {
          out.put(a, "true");
        }
      }
    }
    return out;
  }
}
