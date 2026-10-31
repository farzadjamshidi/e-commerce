package com.ecommerce.order;

import java.math.BigDecimal;
import java.util.Optional;
import java.util.UUID;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
class OrderService {

    private final OrderRepository orderRepository;

    OrderService(OrderRepository orderRepository) {
        this.orderRepository = orderRepository;
    }

    @Transactional
    Order placeOrder(String customerId, String sku, int quantity, BigDecimal unitPrice, String currency) {
        return orderRepository.save(Order.place(customerId, sku, quantity, unitPrice, currency));
    }

    @Transactional(readOnly = true)
    Optional<Order> findOrder(UUID id) {
        return orderRepository.findById(id);
    }
}
