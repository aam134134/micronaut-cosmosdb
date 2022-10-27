package com.example.micronaut.cosmosdb;

import com.azure.cosmos.*;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.CosmosContainerResponse;
import com.azure.cosmos.models.CosmosDatabaseResponse;
import com.azure.cosmos.models.ThroughputProperties;
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

    CosmosClient cosmosClient;

    CosmosDatabase database;

    CosmosContainer container;

    CosmosAsyncClient cosmosAsyncClient;

    CosmosAsyncDatabase asyncDatabase;

    CosmosAsyncContainer asyncContainer;

    @Container
    public CosmosDBEmulatorContainer cosmos = new CosmosDBEmulatorContainer(DockerImageName.parse("mcr.microsoft.com/cosmosdb/linux/azure-cosmos-emulator:latest"));

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

    void setClient() {
        cosmosClient = new CosmosClientBuilder()
                .gatewayMode()
                .endpointDiscoveryEnabled(false)
                .endpoint(cosmos.getEmulatorEndpoint())
                .key(cosmos.getEmulatorKey())
                .buildClient();

        cosmosAsyncClient = new CosmosClientBuilder()
                .gatewayMode()
                .endpointDiscoveryEnabled(false)
                .endpoint(cosmos.getEmulatorEndpoint())
                .key(cosmos.getEmulatorKey())
                .buildAsyncClient();
    }

    void initDb() {
        if (database != null) {
            database.delete();
        }

        final CosmosDatabaseResponse cosmosDatabaseResponse = cosmosClient.createDatabaseIfNotExists(getDbName());
        database = cosmosClient.getDatabase(cosmosDatabaseResponse.getProperties().getId());

        asyncDatabase = cosmosAsyncClient.getDatabase(cosmosDatabaseResponse.getProperties().getId());
    }

    void initContainer() {

        // a Container is  like a DB table/Elasticsearch Index
        // partition key is a field in a stored document;
        // CosmosDB uses this key to assign which partition an item/doc will be stored;
        // the partition key should be a value that is as random as possible (uuid etc.)
        final CosmosContainerResponse cosmosContainerResponse = database.createContainerIfNotExists(new CosmosContainerProperties(getContainerName(), "/name"), ThroughputProperties.createManualThroughput(400));
        container = database.getContainer(cosmosContainerResponse.getProperties().getId());

        asyncContainer = asyncDatabase.getContainer(cosmosContainerResponse.getProperties().getId());
    }
}
