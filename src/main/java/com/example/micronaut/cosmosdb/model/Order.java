package com.example.micronaut.cosmosdb.model;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
public class Order {

    // 'id' is a required field in Cosmos DB
    String id;

    String name;

    String orderNum;
}
