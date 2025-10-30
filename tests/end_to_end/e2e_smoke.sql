-- Bored end-to-end smoke script
-- This script exercises database creation, schema setup, basic DDL, and core DML paths.

-- Ensure a clean slate when the script is rerun.
DROP DATABASE IF EXISTS bored_e2e;

-- Create the target database and schema for the smoke scenario.
CREATE DATABASE bored_e2e;
CREATE SCHEMA bored_e2e.app;

-- Define an inventory table with representative column types.
CREATE TABLE bored_e2e.app.inventory (
    id INT NOT NULL,
    sku TEXT NOT NULL,
    quantity INT NOT NULL,
    PRIMARY KEY (id)
);

-- Seed the table with a small batch of rows.
INSERT INTO bored_e2e.app.inventory (id, sku, quantity) VALUES
    (1, 'widget', 5),
    (2, 'gadget', 3),
    (3, 'sprocket', 9);

-- Exercise UPDATE and DELETE paths to mutate the seeded data.
UPDATE bored_e2e.app.inventory
SET quantity = quantity + 1
WHERE id = 2;

DELETE FROM bored_e2e.app.inventory
WHERE id = 3;

-- Query the remaining rows to validate DDL + DML effects.
SELECT id, sku, quantity
FROM bored_e2e.app.inventory
ORDER BY id;

-- Optional clean-up so repeated runs behave deterministically.
DROP DATABASE bored_e2e;
