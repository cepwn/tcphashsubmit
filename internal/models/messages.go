package models

import "encoding/json"

type BasePayload struct {
	ID *int `json:"id"`
}

type Response struct {
	BasePayload
	Result bool    `json:"result"`
	Error  *string `json:"error,omitempty"`
}

type BaseRequest struct {
	BasePayload
	Method string `json:"method"`
}

type RawRequest struct {
	BaseRequest
	Params json.RawMessage `json:"params"`
}

type AuthenticationRequest struct {
	BaseRequest
	Params AuthenticationRequestParams `json:"params"`
}

type AuthenticationRequestParams struct {
	Username string `json:"username"`
}

type TaskAssignmentRequest struct {
	BaseRequest
	Params TaskAssignmentRequestParams `json:"params"`
}

type TaskAssignmentRequestParams struct {
	JobID       int    `json:"job_id"`
	ServerNonce string `json:"server_nonce"`
}

type ResultSubmissionRequest struct {
	BaseRequest
	Params ResultSubmissionRequestParams `json:"params"`
}

type ResultSubmissionRequestParams struct {
	JobID       int    `json:"job_id"`
	ClientNonce string `json:"client_nonce"`
	Result      string `json:"result"`
}
