-- +goose Up
ALTER TABLE tariffs ADD COLUMN speed_limit BIGINT;

-- +goose Down
ALTER TABLE tariffs DROP COLUMN speed_limit;
