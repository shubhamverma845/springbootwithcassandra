package com.shubham.betterreadsdataloader.connection;


import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.io.File;

@ConfigurationProperties(prefix = "datastax.astra")
@Component
@Getter
@Setter
public class DataStaxAstraProperties {
    private File secureConnectBundle;
}

