package com.ecommerce.payment;

enum PaymentStatus {
    COMPLETED,
    FAILED,
    // Charged, then the order was cancelled
    REFUNDED,
    // The order was cancelled before any charge
    CANCELLED
}
