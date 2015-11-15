package com.manywho.services.backend.configuration;

import com.manywho.sdk.services.annotations.Property;

public class Configuration {
    public static final String DATABASE_URL = "Database Url";
    public static final String DATABASE_USERNAME = "Database Username";
    public static final String DATABASE_PASSWORD = "Database Password";

    @Property(DATABASE_URL)
    private String url;

    @Property(DATABASE_USERNAME)
    private String username;

    @Property(DATABASE_PASSWORD)
    private String password;

    public String getUrl() {
        return url;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }
}
