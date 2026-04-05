-- Replica database — mirrors source schema.
-- StreamPipe upserts rows here; no seed data needed.

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS public.users (
    id          UUID        PRIMARY KEY,
    name        TEXT        NOT NULL,
    email       TEXT        NOT NULL UNIQUE,
    phone       TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.products (
    id          UUID        PRIMARY KEY,
    name        TEXT        NOT NULL,
    price_cents INTEGER     NOT NULL,
    stock       INTEGER     NOT NULL DEFAULT 0,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.orders (
    id          UUID        PRIMARY KEY,
    user_id     UUID        NOT NULL,
    product_id  UUID        NOT NULL,
    quantity    INTEGER     NOT NULL DEFAULT 1,
    status      TEXT        NOT NULL DEFAULT 'pending',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
