package com.example.micronaut.cosmosdb;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosDatabase;
import com.azure.cosmos.models.*;
import com.azure.cosmos.util.CosmosPagedIterable;
import com.example.micronaut.cosmosdb.model.Order;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.stream.Collectors;

public class CosmosDbSyncTest extends CosmosDbTest {

    public CosmosClient cosmosClient;

    private CosmosDatabase database;

    private CosmosContainer container;

    @Test
    public void testReadItem() {

        // ID is a required field in Cosmos DB
        // we use it to do readItem()

        final Order o = new Order();
        o.setId("a1");
        o.setName("bill");
        o.setOrderNum("1234");
        Assertions.assertEquals(201, container.createItem(o).getStatusCode());

        final CosmosItemResponse<Order> cosmosItemResponse = container.readItem(o.getId(), new PartitionKey(o.getName()), Order.class);
        Assertions.assertEquals(o.getOrderNum(), cosmosItemResponse.getItem().getOrderNum());
    }

    @Test
    public void testQueryItems() {
        final Order o = new Order();
        o.setId("a1");
        o.setName("bill");
        o.setOrderNum("1234");
        Assertions.assertEquals(201, container.createItem(o).getStatusCode());

        /**
         * Notice how the SQL query refers to the plural of the JSON object - Orders (from Order.class)
         * CosmosDB automatically makes it plural; not sure how/if that can be configured
         */
        final CosmosPagedIterable<Order> cosmosPagedIterable = container.queryItems(
                "SELECT * FROM Orders WHERE Orders.name IN ('bill')", new CosmosQueryRequestOptions(), Order.class);
        Assertions.assertTrue(cosmosPagedIterable.stream().count() == 1);

        cosmosPagedIterable.iterableByPage(10).forEach(cosmosItemPropertiesFeedResponse -> {
            System.out.println("Result size: " + cosmosItemPropertiesFeedResponse.getResults().size());

            System.out.println("Item Ids " + cosmosItemPropertiesFeedResponse
                    .getResults()
                    .stream()
                    .map(Order::getId)
                    .collect(Collectors.toList()));
        });
    }

    @Override
    void setClient() {
        cosmosClient = new CosmosClientBuilder()
                .gatewayMode()
                .endpointDiscoveryEnabled(false)
                .endpoint(cosmos.getEmulatorEndpoint())
                .key(cosmos.getEmulatorKey())
                .buildClient();
    }

    @Override
    void initDb() {
        if (database != null) {
            database.delete();
        }

        final CosmosDatabaseResponse cosmosDatabaseResponse = cosmosClient.createDatabaseIfNotExists(getDbName());
        database = cosmosClient.getDatabase(cosmosDatabaseResponse.getProperties().getId());
    }

    @Override
    void initContainer() {

        // a Container is  like a DB table/Elasticsearch Index
        // partition key is a field in a stored document;
        // CosmosDB uses this key to assign which partition an item/doc will be stored;
        // the partition key should be a value that is as random as possible (uuid etc.)
        final CosmosContainerResponse cosmosContainerResponse = database.createContainerIfNotExists(new CosmosContainerProperties(getContainerName(), "/name"), ThroughputProperties.createManualThroughput(400));
        container = database.getContainer(cosmosContainerResponse.getProperties().getId());
    }
}
