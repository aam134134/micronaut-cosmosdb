package com.example.micronaut.cosmosdb;

import com.azure.cosmos.models.CosmosItemResponse;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.util.CosmosPagedFlux;
import com.example.micronaut.cosmosdb.model.Order;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.util.Optional;

public class CosmosDbAsyncTest extends CosmosDbTest {

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

        /**
         * Can use anything for the "table" name
         */
        CosmosPagedFlux<Order> pagedFluxResponse = asyncContainer.queryItems(
                "SELECT * FROM Anything a WHERE a.name IN ('bill')", Order.class);
        Assertions.assertTrue(pagedFluxResponse.byPage(10).hasElements().block().booleanValue());

        pagedFluxResponse.byPage(10).flatMap(fluxResponse -> {
            Assertions.assertEquals(1, fluxResponse.getResults().size());
            Assertions.assertTrue(fluxResponse.getResults().stream().allMatch(order -> order.getId().equals(o.getId())));
            return Flux.empty();
        });
    }
}
