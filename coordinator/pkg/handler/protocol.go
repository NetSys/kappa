package handler

// This source file has a corresponding, easyjson-generated file named `protocol_easyjson.go`.
// If you update this file, don't forget to run `easyjson -all protocol.go`.

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/mailru/easyjson"
)

type PidT int // Overflow is unlikely.
type SeqnoT int

type AppEvT interface{} // Type of application events.

// TODO(zhangwen): using interface{} liberally is worrisome.
type CCResT interface{} // Return type of coordinator calls.

// event contains the format of the event structure sent to handlers.
type event struct {
	Pid             PidT   `json:"pid"`
	Seqno           SeqnoT `json:"seqno"`
	ChkID           string `json:"chk_id"`
	CoordCallResult CCResT `json:"coord_call_result"`
	AppEvent        AppEvT `json:"app_event"`
}

// CoordCall is the format of a single coordinator call inside a request to the coordinator.
type CoordCall struct {
	Seqno  SeqnoT              `json:"seqno"`
	Op     string              `json:"op"`
	Params easyjson.RawMessage `json:"params"`
}

// Request is the format of a handler's request to the coordinator.
type Request struct {
	Pid     PidT        `json:"pid"`
	Seqno   SeqnoT      `json:"seqno"`
	ChkID   string      `json:"chk_id"`
	Calls   []CoordCall `json:"calls"`
	Blocked bool        `json:"blocked"`
	Err     *string     `json:"err"`
}

func ParseRequest(b []byte) (*Request, error) {
	var req Request
	if err := easyjson.Unmarshal(b, &req); err != nil {
		err = fmt.Errorf("protocol.ParseRequest: %v: %s", err, string(b))
		return nil, err
	}

	if req.Err != nil {
		log.Println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
		log.Println("[!!!!! WARNING !!!!!] protocol.ParseRequest:", *req.Err) // Not fatal.
		log.Println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
	}

	// Sanity check: the request's seqno is >= any single call's seqno.
	for _, call := range req.Calls {
		if call.Seqno > req.Seqno {
			return nil, fmt.Errorf("protocol.ParseRequest: seqno out of range: %d > %d", call.Seqno, req.Seqno)
		}
	}

	return &req, nil
}

func EncodeCoordCallResult(res CCResT) ([]byte, error) {
	return json.Marshal(res)
}
