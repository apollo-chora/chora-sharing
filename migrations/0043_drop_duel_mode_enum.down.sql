-- Recreate the dead enum for rollback completeness (it was never used).
CREATE TYPE duel_mode AS ENUM ('topic_ai', 'pool');
