package models

type Session struct {
	Username          string
	Authenticated     bool
	LatestJobID       int
	LatestServerNonce string
	SeenClientNonces  map[string]struct{}
	JobHistory        map[int]string
}
