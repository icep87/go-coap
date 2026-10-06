package client

import (
	"errors"
	"fmt"

	"github.com/plgd-dev/go-coap/v3/message"
	"github.com/plgd-dev/go-coap/v3/message/codes"
	"github.com/plgd-dev/go-coap/v3/message/pool"
	"github.com/plgd-dev/go-coap/v3/net/qblock"
	"github.com/plgd-dev/go-coap/v3/net/responsewriter"
)

func q1ControlFromResponse(msg *pool.Message, blockCount uint32) (qblock.Control, bool, error) {
	size, err := msg.BodySize()
	if err != nil || size < 0 || uint64(size) > uint64(^uint32(0)) {
		return qblock.Control{}, true, qblock.ErrLimitExceeded
	}
	return q1ControlFromResponseBounded(msg, blockCount, uint32(size), blockCount)
}

func q1ControlFromResponseBounded(msg *pool.Message, blockCount, maxBytes, maxMissing uint32) (qblock.Control, bool, error) {
	control := qblock.Control{Token: message.Token(append([]byte(nil), msg.Token()...))}
	switch msg.Code() {
	case codes.Continue:
		if msg.HasOption(message.QBlock2) {
			return control, true, errQBlockMixedResponseOptions
		}
		if err := qblock.ValidateOptions(msg.Options(), true); err != nil {
			return control, true, err
		}
		if qblockOptionCount(msg, message.QBlock1) != 1 {
			return control, true, errors.New("q-block continue response requires one QBlock1")
		}
		value, err := msg.GetOptionUint32(message.QBlock1)
		if err != nil {
			return control, true, err
		}
		block, err := qblock.DecodeBlock(value)
		if err != nil {
			return control, true, err
		}
		control.Continue = &block.Number
		return control, true, nil
	case codes.RequestEntityIncomplete:
		if msg.HasOption(message.QBlock1) || msg.HasOption(message.QBlock2) {
			return control, true, errQBlockMixedResponseOptions
		}
		if qblockOptionCount(msg, message.ContentFormat) != 1 {
			return control, true, errors.New("q-block missing response requires Content-Format")
		}
		contentFormat, err := msg.ContentFormat()
		if err != nil {
			return control, true, err
		}
		if contentFormat != message.AppMissingBlocksCBORSeq {
			return control, true, errors.New("q-block missing response requires missing-blocks+cbor-seq")
		}
		body := msg.Body()
		if body == nil {
			return control, true, errors.New("q-block missing response requires payload")
		}
		payload, err := readQBlockBody(body, maxBytes)
		if err != nil {
			return control, true, err
		}
		missing, err := qblock.DecodeMissing(payload, blockCount, int(min(blockCount, maxMissing)))
		if err != nil {
			return control, true, err
		}
		control.Missing = missing
		return control, true, nil
	default:
		if msg.HasOption(message.QBlock1) || msg.HasOption(message.QBlock2) {
			return control, true, errors.New("unexpected q-block option in terminal response")
		}
		return control, false, nil
	}
}

func isMixedQBlockOptions(opts message.Options) bool {
	hasQ := opts.HasOption(message.QBlock1) || opts.HasOption(message.QBlock2)
	hasClassic := opts.HasOption(message.Block1) || opts.HasOption(message.Block2)
	return hasQ && hasClassic
}

// handleQBlockServerRequestError rejects requests that the Q-Block server
// cannot process safely. A nil cause classifies mixed Q-Block and classic
// Block options; parser errors are handled only when they have an explicit
// wire response policy.
func (cc *Conn) handleQBlockServerRequestError(w *responsewriter.ResponseWriter[*Conn], req *pool.Message, cause error) bool {
	if req.Code() < codes.GET || req.Code() >= codes.Code(32) {
		return false
	}

	var responseCode codes.Code
	switch {
	case cause == nil && isMixedQBlockOptions(req.Options()):
		responseCode = codes.BadOption
	case errors.Is(cause, qblock.ErrMixedOptions):
		responseCode = codes.BadOption
	case errors.Is(cause, errQBlock2SelectorOrder):
		if req.Type() != message.NonConfirmable || !req.HasOption(message.QBlock2) {
			return false
		}
		responseCode = codes.BadRequest
	case cause == nil && req.Type() == message.Confirmable && (req.HasOption(message.QBlock1) || req.HasOption(message.QBlock2)):
		// The disabled path historically rejects CON Q requests with Bad
		// Option, even when their Q options are otherwise valid.
		responseCode = codes.BadOption
	case cause != nil && req.Type() == message.Confirmable:
		// The disabled path historically rejects malformed CON Q requests with
		// Bad Option. Keep that policy while sharing response formation/cache.
		responseCode = codes.BadOption
	default:
		return false
	}

	if req.Type() != message.Confirmable && req.Type() != message.NonConfirmable {
		return true
	}
	resp := w.Message()
	resp.SetCode(responseCode)
	resp.SetToken(req.Token())
	if req.Type() == message.Confirmable {
		resp.SetType(message.Acknowledgement)
		resp.SetMessageID(req.MessageID())
	} else {
		resp.SetType(message.NonConfirmable)
		resp.SetMessageID(cc.GetMessageID())
	}
	if err := cc.addResponseToCacheForMID(req.MessageID(), resp); err != nil {
		cc.errors(fmt.Errorf("cannot cache Q-Block rejection: %w", err))
	}
	return true
}

// handleDisabledQBlock prevents unsupported fragments reaching application handlers.
// Responses are not requests and must never elicit a Bad Option response.
func (cc *Conn) handleDisabledQBlock(w *responsewriter.ResponseWriter[*Conn], req *pool.Message) bool {
	opts := req.Options()
	if !opts.HasOption(message.QBlock1) && !opts.HasOption(message.QBlock2) {
		return false
	}
	request := req.Code() >= codes.GET && req.Code() < codes.Code(32)
	// The enabled private Q-Block client validates and consumes Q2 responses.
	// It also owns Q1 control responses for active private upload tokens.
	// Other Q responses remain unsupported and never elicit another response.
	if !request {
		if cc.qblockClient != nil && (opts.HasOption(message.QBlock2) || cc.qblockClient.ownsQ1Response(req)) {
			return false
		}
		return true
	}
	validationErr := qblock.ValidateOptions(opts, request)
	if req.Type() != message.Confirmable && req.Type() != message.NonConfirmable {
		return true
	}
	if req.Type() == message.NonConfirmable && !errors.Is(validationErr, qblock.ErrMixedOptions) {
		return true
	}
	// Use direct message setters so No-Response cannot suppress a critical-option error.
	return cc.handleQBlockServerRequestError(w, req, validationErr)
}
