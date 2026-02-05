-- +goose Up
-- +goose StatementBegin
CREATE TABLE submissions (
    username VARCHAR(255),
    timestamp TIMESTAMP,
    submission_count INT
);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE submissions;
-- +goose StatementEnd
