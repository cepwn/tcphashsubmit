package store

import (
	"database/sql"
	"time"
)

type Submission struct {
	Username        string
	Timestamp       time.Time
	SubmissionCount int
}

type PostgresSubmissionStore struct {
	db *sql.DB
}

func NewPostgresSubmissionStore(db *sql.DB) *PostgresSubmissionStore {
	return &PostgresSubmissionStore{db}
}

type SubmissionStore interface {
	RecordSubmission(username string, timestamp time.Time) error
	GetSubmissions(username string) ([]Submission, error)
}

func (s *PostgresSubmissionStore) RecordSubmission(username string, timestamp time.Time) error {
	truncated := timestamp.Truncate(time.Minute)

	_, err := s.db.Exec(`
        INSERT INTO submissions (username, timestamp, submission_count)
        VALUES ($1, $2, 1)
        ON CONFLICT (username, timestamp)
        DO UPDATE SET submission_count = submissions.submission_count + 1
    `, username, truncated)

	return err
}

func (s *PostgresSubmissionStore) GetSubmissions(username string) ([]Submission, error) {
	rows, err := s.db.Query(`
        SELECT timestamp, submission_count FROM submissions WHERE username = $1 ORDER BY timestamp DESC
    `, username)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var submissions []Submission
	for rows.Next() {
		var sub Submission
		sub.Username = username
		err := rows.Scan(&sub.Timestamp, &sub.SubmissionCount)
		if err != nil {
			return nil, err
		}
		submissions = append(submissions, sub)
	}
	return submissions, rows.Err()
}
