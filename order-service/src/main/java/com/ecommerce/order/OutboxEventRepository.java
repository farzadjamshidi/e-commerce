package com.ecommerce.order;

import java.util.UUID;

import org.springframework.data.jpa.repository.JpaRepository;

interface OutboxEventRepository extends JpaRepository<OutboxEvent, UUID> {
}
