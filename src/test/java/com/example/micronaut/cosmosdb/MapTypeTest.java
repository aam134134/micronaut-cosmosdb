package com.example.micronaut.cosmosdb;

import com.azure.cosmos.models.CosmosItemResponse;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.FeedResponse;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.util.CosmosPagedIterable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class MapTypeTest extends CosmosDbTest {

    @Test
    public void testReadItem() {

        // ID is a required field in Cosmos DB
        // we use it to do readItem()

        final Map<String, String> o = new HashMap();
        o.put("id", "a1");
        o.put("name", "bill");
        o.put("orderNum", "1234");
        Assertions.assertEquals(201, container.createItem(o).getStatusCode());

        final CosmosItemResponse<Map> cosmosItemResponse = container.readItem(o.get("id"), new PartitionKey(o.get("name")), Map.class);
        Assertions.assertEquals(o.get("orderNum"), cosmosItemResponse.getItem().get("orderNum"));
    }

    @Test
    public void testQueryItems() {
        final Map<String, String> o = new HashMap();
        o.put("id", "a1");
        o.put("name", "bill");
        o.put("orderNum", "1234");
        Assertions.assertEquals(201, container.createItem(o).getStatusCode());

        /**
         * Can use anything for the "table" name
         */
        final CosmosPagedIterable<Map> cosmosPagedIterable = container.queryItems(
                "SELECT * FROM AnyTableName m WHERE m.name IN ('bill')", new CosmosQueryRequestOptions(), Map.class);
        Assertions.assertTrue(cosmosPagedIterable.stream().count() == 1);

        final Iterable<FeedResponse<Map>> responseIterable = cosmosPagedIterable.iterableByPage(10);
        final FeedResponse<Map> response = responseIterable.iterator().next();

        Assertions.assertEquals(1, response.getResults().size());
        Assertions.assertEquals("a1", response.getResults().iterator().next().get("id"));
    }
}
