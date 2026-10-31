package com.ecommerce.order;

import java.math.BigDecimal;
import java.net.URI;
import java.time.Instant;
import java.util.UUID;

import jakarta.validation.Valid;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Digits;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/orders")
class OrderController {

    private final OrderService orderService;

    OrderController(OrderService orderService) {
        this.orderService = orderService;
    }

    @PostMapping
    ResponseEntity<OrderResponse> placeOrder(@Valid @RequestBody PlaceOrderRequest request) {
        Order order = orderService.placeOrder(
                request.customerId(), request.sku(), request.quantity(), request.unitPrice(), request.currency());
        return ResponseEntity.accepted()
                .location(URI.create("/orders/" + order.getId()))
                .body(OrderResponse.from(order));
    }

    @GetMapping("/{id}")
    ResponseEntity<OrderResponse> getOrder(@PathVariable UUID id) {
        return orderService.findOrder(id)
                .map(order -> ResponseEntity.ok(OrderResponse.from(order)))
                .orElse(ResponseEntity.notFound().build());
    }

    record PlaceOrderRequest(
            @NotBlank @Size(max = 64) String customerId,
            @NotBlank @Size(max = 64) String sku,
            @Min(1) int quantity,
            @NotNull @DecimalMin("0.00") @Digits(integer = 10, fraction = 2) BigDecimal unitPrice,
            @NotNull @Pattern(regexp = "[A-Z]{3}") String currency) {
    }

    record OrderResponse(
            UUID orderId, String customerId, String sku, int quantity,
            BigDecimal totalAmount, String currency, OrderStatus status, Long version, Instant createdAt) {

        static OrderResponse from(Order order) {
            return new OrderResponse(order.getId(), order.getCustomerId(), order.getSku(), order.getQuantity(),
                    order.getTotalAmount(), order.getCurrency(), order.getStatus(), order.getVersion(),
                    order.getCreatedAt());
        }
    }
}
