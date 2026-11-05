package com.ecommerce.order;

enum SagaState {
    RESERVING_STOCK,
    CHARGING_PAYMENT,
    RELEASING_STOCK,
    COMPLETED,
    CANCELLED
}
