package com.ecommerce.order;

import java.util.HashMap;
import java.util.Map;

import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import org.springframework.stereotype.Component;

// Moves trace context in and out of the database, where no thread can carry it
@Component
class TraceContext {

    private static final String HEADER = "traceparent";

    private final Tracer tracer;
    private final Propagator propagator;

    TraceContext(Tracer tracer, Propagator propagator) {
        this.tracer = tracer;
        this.propagator = propagator;
    }

    String current() {
        Span span = tracer.currentSpan();
        if (span == null) {
            return null;
        }
        Map<String, String> headers = new HashMap<>();
        propagator.inject(span.context(), headers, Map::put);
        return headers.get(HEADER);
    }

    // Runs the action in a new span under a stored context, so later work joins the original trace
    void runIn(String traceparent, String spanName, Runnable action) {
        Span.Builder builder = traceparent == null
                ? tracer.spanBuilder()
                : propagator.extract(Map.of(HEADER, traceparent), Map::get);
        Span span = builder.name(spanName).start();
        try (Tracer.SpanInScope scope = tracer.withSpan(span)) {
            action.run();
        } catch (RuntimeException e) {
            span.error(e);
            throw e;
        } finally {
            span.end();
        }
    }
}
