package com.zeotap.fanout.core.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public final class ConfigLoader {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private ConfigLoader() {}

  public static AppConfig load(Path path) {
    try {
      byte[] bytes = Files.readAllBytes(path);
      return MAPPER.readValue(bytes, AppConfig.class);
    } catch (IOException e) {
      throw new RuntimeException("Failed to load config: " + path.toAbsolutePath(), e);
    }
  }
}
