ALTER TABLE duel_rounds
    DROP COLUMN IF EXISTS question,
    DROP COLUMN IF EXISTS options,
    DROP COLUMN IF EXISTS correct_answer,
    DROP COLUMN IF EXISTS challenger_answered,
    DROP COLUMN IF EXISTS opponent_answered;
