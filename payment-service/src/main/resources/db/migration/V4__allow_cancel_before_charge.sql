-- A cancel can arrive before InventoryReserved. Its row has no amount yet, and it blocks the later charge.
ALTER TABLE payments
    ALTER COLUMN customer_id DROP NOT NULL,
    ALTER COLUMN amount DROP NOT NULL,
    ALTER COLUMN currency DROP NOT NULL,
    ADD COLUMN updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    ADD CONSTRAINT payments_charge_data CHECK (
        status = 'CANCELLED' OR (customer_id IS NOT NULL AND amount IS NOT NULL AND currency IS NOT NULL));
