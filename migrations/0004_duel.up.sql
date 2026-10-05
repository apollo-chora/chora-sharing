-- ============================================================================
-- chora-sharing : 0004_duel_v2.up.sql
--
-- Clean greenfield duel implementation — no version suffix in the name.
-- Tables: duel_sessions, duel_rounds, duel_ratings.
-- ============================================================================

CREATE TABLE duel_sessions (
    id                   UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id            UUID         NOT NULL,
    challenger_gcid      UUID         NOT NULL,
    opponent_gcid        UUID         NOT NULL,
    status               duel_status  NOT NULL DEFAULT 'pending',
    scope                duel_scope   NOT NULL DEFAULT 'friendly',
    winner_gcid          UUID,
    round_count          INTEGER      NOT NULL DEFAULT 5,
    score_challenger     INTEGER      NOT NULL DEFAULT 0,
    score_opponent       INTEGER      NOT NULL DEFAULT 0,
    combo_challenger     INTEGER      NOT NULL DEFAULT 0,
    combo_opponent       INTEGER      NOT NULL DEFAULT 0,
    interest_tags        TEXT[]       NOT NULL DEFAULT '{}',
    created_at           TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ  NOT NULL DEFAULT now(),
    completed_at         TIMESTAMPTZ,
    expires_at           TIMESTAMPTZ
);

CREATE INDEX duel_sessions_pair_idx
    ON duel_sessions (challenger_gcid, opponent_gcid);
CREATE INDEX duel_sessions_ranked_completed_idx
    ON duel_sessions (tenant_id, scope, completed_at DESC)
    WHERE scope = 'ranked';

ALTER TABLE duel_sessions ENABLE ROW LEVEL SECURITY;
ALTER TABLE duel_sessions FORCE ROW LEVEL SECURITY;
CREATE POLICY duel_sessions_tenant_isolation ON duel_sessions
    FOR ALL
    USING (tenant_id = current_setting('chora.tenant_id', true)::uuid);

CREATE TABLE duel_rounds (
    id                            UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id                     UUID         NOT NULL,
    duel_session_id               UUID         NOT NULL REFERENCES duel_sessions(id) ON DELETE CASCADE,
    round_number                  INTEGER      NOT NULL,
    atom_id                       UUID         NOT NULL,
    atom_revision_id              UUID,
    question                      TEXT,
    options                       TEXT[]       NOT NULL DEFAULT '{}',
    correct_answer                TEXT,
    challenger_answer             TEXT,
    opponent_answer               TEXT,
    challenger_time_ms            INTEGER,
    opponent_time_ms              INTEGER,
    challenger_correct            BOOLEAN,
    opponent_correct              BOOLEAN,
    combo_multiplier_challenger   INTEGER,
    combo_multiplier_opponent     INTEGER,
    points_challenger             INTEGER,
    points_opponent               INTEGER,
    challenger_answered           BOOLEAN      NOT NULL DEFAULT false,
    opponent_answered             BOOLEAN      NOT NULL DEFAULT false,
    winner_gcid                   UUID,
    resolved_at                   TIMESTAMPTZ,
    created_at                    TIMESTAMPTZ  NOT NULL DEFAULT now(),
    UNIQUE (duel_session_id, round_number)
);

CREATE INDEX duel_rounds_resolved_idx
    ON duel_rounds (duel_session_id, round_number)
    WHERE resolved_at IS NOT NULL;

ALTER TABLE duel_rounds ENABLE ROW LEVEL SECURITY;
ALTER TABLE duel_rounds FORCE ROW LEVEL SECURITY;
CREATE POLICY duel_rounds_tenant_isolation ON duel_rounds
    FOR ALL
    USING (tenant_id = current_setting('chora.tenant_id', true)::uuid);

CREATE TABLE duel_ratings (
    id           UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id    UUID        NOT NULL,
    gcid         UUID        NOT NULL,
    rating       INTEGER     NOT NULL DEFAULT 1200,
    wins         INTEGER     NOT NULL DEFAULT 0,
    losses       INTEGER     NOT NULL DEFAULT 0,
    draws        INTEGER     NOT NULL DEFAULT 0,
    last_duel_at TIMESTAMPTZ,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted_at   TIMESTAMPTZ
);

CREATE UNIQUE INDEX duel_ratings_active_unique_idx
    ON duel_ratings (tenant_id, gcid)
    WHERE deleted_at IS NULL;

CREATE INDEX duel_ratings_tenant_rating_idx
    ON duel_ratings (tenant_id, rating DESC)
    WHERE deleted_at IS NULL;

CREATE TRIGGER duel_ratings_set_updated_at
    BEFORE UPDATE ON duel_ratings
    FOR EACH ROW EXECUTE FUNCTION circle_set_updated_at();

ALTER TABLE duel_ratings ENABLE ROW LEVEL SECURITY;
ALTER TABLE duel_ratings FORCE ROW LEVEL SECURITY;
CREATE POLICY duel_ratings_tenant_isolation ON duel_ratings
    FOR ALL
    USING (tenant_id = current_setting('chora.tenant_id', true)::uuid);
