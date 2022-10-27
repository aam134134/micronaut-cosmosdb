package com.example.micronaut.cosmosdb;

import io.micronaut.runtime.EmbeddedApplication;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.CosmosDBEmulatorContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;

@MicronautTest
@Testcontainers
public abstract class CosmosDbTest {
    @Inject
    EmbeddedApplication<?> application;

    @Container
    public CosmosDBEmulatorContainer cosmos = new CosmosDBEmulatorContainer(DockerImageName.parse("mcr.microsoft.com/cosmosdb/linux/azure-cosmos-emulator:latest"));

    abstract void setClient();

    abstract void initDb();

    abstract void initContainer();

    @BeforeEach
    public void setupCosmosDbTest() throws CertificateException, IOException, KeyStoreException, NoSuchAlgorithmException {
        setupCerts();
        setClient();
        initDb();
        initContainer();
    }

    public String getContainerName() {
        return "OrderContainer";
    }

    public String getDbName() {
        return "OrderDatabase";
    }

    @Test
    void testItWorks() {
        Assertions.assertTrue(application.isRunning());
    }

    void setupCerts() throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException {
        final Path keyStoreFile = new File("azure-cosmos-emulator.keystore").toPath();
        final KeyStore keyStore = cosmos.buildNewKeyStore();
        keyStore.store(new FileOutputStream(keyStoreFile.toFile()), cosmos.getEmulatorKey().toCharArray());

        System.setProperty("javax.net.ssl.trustStore", keyStoreFile.toString());
        System.setProperty("javax.net.ssl.trustStorePassword", cosmos.getEmulatorKey());
        System.setProperty("javax.net.ssl.trustStoreType", "PKCS12");
    }
}
