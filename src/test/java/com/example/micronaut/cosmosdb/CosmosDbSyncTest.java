package com.example.micronaut.cosmosdb;

import com.azure.cosmos.models.CosmosItemResponse;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.FeedResponse;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.util.CosmosPagedIterable;
import com.example.micronaut.cosmosdb.model.Order;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CosmosDbSyncTest extends CosmosDbTest {

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

        final Iterable<FeedResponse<Order>> responseIterable = cosmosPagedIterable.iterableByPage(10);
        final FeedResponse<Order> response = responseIterable.iterator().next();

        Assertions.assertEquals(1, response.getResults().size());
        Assertions.assertEquals("a1", response.getResults().iterator().next().getId());
    }
}
