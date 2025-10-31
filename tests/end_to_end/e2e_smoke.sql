-- Bored end-to-end smoke script
-- This script exercises database creation, schema setup, basic DDL, and core DML paths.

DROP TABLE IF EXISTS analytics.inventory;

CREATE TABLE analytics.inventory (
    id INT PRIMARY KEY,
    sku TEXT NOT NULL,
    quantity INT NOT NULL
);

-- Seed the table with a small batch of rows.
INSERT INTO analytics.inventory (id, sku, quantity) VALUES
    (1, 'widget', 5),
    (2, 'gadget', 3),
    (3, 'sprocket', 9);

-- Exercise UPDATE and DELETE paths to mutate the seeded data.
UPDATE analytics.inventory
SET quantity = quantity + 1
WHERE id = 2;

DELETE FROM analytics.inventory
WHERE id = 3;

-- Query the remaining rows to validate DDL + DML effects.
SELECT id, sku, quantity
FROM analytics.inventory
ORDER BY id;

-- Optional clean-up so repeated runs behave deterministically.
DROP TABLE analytics.inventory;
