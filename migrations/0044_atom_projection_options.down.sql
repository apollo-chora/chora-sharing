-- 0044_atom_projection_options.down.sql

ALTER TABLE atom_projections
    DROP COLUMN IF EXISTS correct_answer,
    DROP COLUMN IF EXISTS options;
