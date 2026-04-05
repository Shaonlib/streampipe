CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS public.users (
    id          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    name        TEXT        NOT NULL,
    email       TEXT        NOT NULL UNIQUE,
    phone       TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.products (
    id          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    name        TEXT        NOT NULL,
    price_cents INTEGER     NOT NULL CHECK (price_cents >= 0),
    stock       INTEGER     NOT NULL DEFAULT 0,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.orders (
    id          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id     UUID        NOT NULL REFERENCES public.users(id),
    product_id  UUID        NOT NULL REFERENCES public.products(id),
    quantity    INTEGER     NOT NULL DEFAULT 1,
    status      TEXT        NOT NULL DEFAULT 'pending'
                            CHECK (status IN ('pending','paid','shipped','cancelled')),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE public.users    REPLICA IDENTITY FULL;
ALTER TABLE public.products REPLICA IDENTITY FULL;
ALTER TABLE public.orders   REPLICA IDENTITY FULL;

INSERT INTO public.users (name, email, phone) VALUES
    ('Alice Nguyen',  'alice@example.com',  '+1-555-0101'),
    ('Bob Kowalski',  'bob@example.com',    '+1-555-0102'),
    ('Clara Schmidt', 'clara@example.com',  '+1-555-0103');

INSERT INTO public.products (name, price_cents, stock) VALUES
    ('Mechanical Keyboard', 12900, 50),
    ('USB-C Hub',            4999, 120),
    ('Webcam HD',            7999,  30);

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'replicator') THEN
        CREATE ROLE replicator WITH LOGIN PASSWORD 'replicator_pass' REPLICATION SUPERUSER;
    END IF;
END $$;

GRANT SELECT ON ALL TABLES IN SCHEMA public TO replicator;

CREATE PUBLICATION streampipe_pub FOR TABLE public.users, public.orders, public.products;
