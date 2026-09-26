-- Ads DB (component instance `postgres`, database `app`). Owner: ad-placement (runs the migration).
-- Read by click-receiver (redirect lookup) through POSTGRES_READ_URL, primary on a miss.
-- The diagram's User table is intentionally not built: user_id is an opaque string (spec §12).

CREATE TABLE IF NOT EXISTS advertisers (
    id          bigserial   PRIMARY KEY,
    name        text        NOT NULL CHECK (length(name) BETWEEN 1 AND 200),
    created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ads (
    id             bigserial   PRIMARY KEY,
    advertiser_id  bigint      NOT NULL REFERENCES advertisers (id),
    content        text        NOT NULL CHECK (length(content) BETWEEN 1 AND 2000),
    img_url        text,
    redirect_url   text        NOT NULL,
    active         boolean     NOT NULL DEFAULT true,   -- DELETE = soft delete (active = false)
    created_at     timestamptz NOT NULL DEFAULT now(),
    updated_at     timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ads_advertiser_id_idx ON ads (advertiser_id, id);
CREATE INDEX IF NOT EXISTS ads_active_id_idx ON ads (id) WHERE active;
