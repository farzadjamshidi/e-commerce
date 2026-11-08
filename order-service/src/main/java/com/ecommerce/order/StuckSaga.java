package com.ecommerce.order;

import java.util.UUID;

interface StuckSaga {

    UUID getOrderId();

    String getTraceparent();
}
