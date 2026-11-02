CREATE TABLE stock (
    sku VARCHAR(64) PRIMARY KEY,
    available INT NOT NULL CHECK (available >= 0),
    reserved INT NOT NULL DEFAULT 0 CHECK (reserved >= 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Demo catalog
INSERT INTO stock (sku, available) VALUES
    ('laptop-pro-14', 10),
    ('phone-x', 3),
    ('headphones-z', 0);
