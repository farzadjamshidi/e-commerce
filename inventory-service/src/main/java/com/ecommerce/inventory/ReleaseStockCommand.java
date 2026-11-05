package com.ecommerce.inventory;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.UUID;

@JsonIgnoreProperties(ignoreUnknown = true)
record ReleaseStockCommand(UUID orderId) {

    static final String TYPE = "ReleaseStock";
}
