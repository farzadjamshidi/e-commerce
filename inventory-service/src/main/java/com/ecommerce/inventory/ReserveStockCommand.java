package com.ecommerce.inventory;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.UUID;

@JsonIgnoreProperties(ignoreUnknown = true)
record ReserveStockCommand(UUID orderId, String sku, int quantity) {

    static final String TYPE = "ReserveStock";
}
