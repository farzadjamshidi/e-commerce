package com.ecommerce.order;

import java.util.List;

import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

@Component
class DemoOrderRunner implements ApplicationRunner {

    private final OrderEventProducer producer;

    DemoOrderRunner(OrderEventProducer producer) {
        this.producer = producer;
    }

    @Override
    public void run(ApplicationArguments args) {
        List<String> orderIds = List.of("order-1001", "order-1002", "order-1003");
        List<String> lifecycle = List.of("OrderCreated", "OrderPaid", "OrderShipped");

        for (String event : lifecycle) {
            for (String orderId : orderIds) {
                producer.send(orderId, event);
            }
        }
        producer.flush();
    }
}
