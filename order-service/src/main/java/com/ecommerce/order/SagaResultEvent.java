package com.ecommerce.order;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.UUID;

// The fields order-service needs from InventoryRejected, PaymentCompleted and PaymentFailed
@JsonIgnoreProperties(ignoreUnknown = true)
record SagaResultEvent(UUID orderId, String reason) {
}
