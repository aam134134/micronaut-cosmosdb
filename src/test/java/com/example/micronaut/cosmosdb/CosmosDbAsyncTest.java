package com.example.micronaut.cosmosdb;

import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosAsyncDatabase;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.models.*;
import com.azure.cosmos.util.CosmosPagedFlux;
import com.example.micronaut.cosmosdb.model.Order;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Optional;

public class CosmosDbAsyncTest extends CosmosDbTest {

    public CosmosAsyncClient cosmosAsyncClient;


    private CosmosAsyncDatabase asyncDatabase;

    private CosmosAsyncContainer asyncContainer;

    @Test
    public void testReadItem() {

        // ID is a required field in Cosmos DB
        // we use it to do readItem()

        final Order o = new Order();
        o.setId("a1");
        o.setName("bill");
        o.setOrderNum("1234");
        Assertions.assertEquals(201, asyncContainer.createItem(o).block().getStatusCode());

        final Optional<CosmosItemResponse<Order>> optional = asyncContainer.readItem(o.getId(), new PartitionKey(o.getName()), Order.class).blockOptional();
        Assertions.assertEquals(o.getOrderNum(), optional.orElseThrow().getItem().getOrderNum());
    }

    @Test
    public void testQueryItems() {
        final Order o = new Order();
        o.setId("a1");
        o.setName("bill");
        o.setOrderNum("1234");
        Assertions.assertEquals(201, asyncContainer.createItem(o).block().getStatusCode());

        CosmosPagedFlux<Order> pagedFluxResponse = asyncContainer.queryItems(
                "SELECT * FROM Orders WHERE Orders.name IN ('bill')", Order.class);
        Assertions.assertTrue(pagedFluxResponse.byPage(10).hasElements().block().booleanValue());

        pagedFluxResponse.byPage(10).flatMap(fluxResponse -> {
            Assertions.assertEquals(1, fluxResponse.getResults().size());
            Assertions.assertTrue(fluxResponse.getResults().stream().allMatch(order -> order.getId().equals(o.getId())));
            return Flux.empty();
        });
    }

    @Override
    public void setClient() {
        cosmosAsyncClient = new CosmosClientBuilder()
                .gatewayMode()
                .endpointDiscoveryEnabled(false)
                .endpoint(cosmos.getEmulatorEndpoint())
                .key(cosmos.getEmulatorKey())
                .buildAsyncClient();
    }

    @Override
    void initDb() {
        if (asyncDatabase != null) {
            asyncDatabase.delete().block();
        }

        Mono<CosmosDatabaseResponse> databaseIfNotExists = cosmosAsyncClient.createDatabaseIfNotExists(getDbName());
        databaseIfNotExists.flatMap(databaseResponse -> {
            asyncDatabase = cosmosAsyncClient.getDatabase(databaseResponse.getProperties().getId());
            return Mono.empty();
        }).block();
    }

    @Override
    void initContainer() {

        // a Container is  like a DB table/Elasticsearch Index
        // partition key is a field in a stored document;
        // CosmosDB uses this key to assign which partition an item/doc will be stored;
        // the partition key should be a value that is as random as possible (uuid etc.)
        final Mono<CosmosContainerResponse> containerIfNotExists = asyncDatabase.createContainerIfNotExists(new CosmosContainerProperties(getContainerName(), "/name"), ThroughputProperties.createManualThroughput(400));
        asyncContainer = asyncDatabase.getContainer(containerIfNotExists.block().getProperties().getId());
    }
}
