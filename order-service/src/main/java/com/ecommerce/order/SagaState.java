package com.ecommerce.order;

enum SagaState {
    RESERVING_STOCK,
    CHARGING_PAYMENT,
    REFUNDING_PAYMENT,
    RELEASING_STOCK,
    COMPLETED,
    CANCELLED
}
