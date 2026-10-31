package com.ecommerce.order;

import java.util.UUID;

import org.springframework.data.jpa.repository.JpaRepository;

interface OrderRepository extends JpaRepository<Order, UUID> {
}
