package riak

import (
	"github.com/titanous/go-riak/pb"
)

func (client *Client) CounterUpdate(bucket, key string, amount int64, options ...map[string]uint32) error {
	retval := true
	req := &pb.RpbCounterUpdateReq{
		Bucket:      []byte(bucket),
		Key:         []byte(key),
		Amount:      &amount,
		Returnvalue: &retval,
	}
	for _, omap := range options {
		for k, v := range omap {
			switch k {
			case "w":
				req.W = &v
			case "dw":
				req.Dw = &v
			case "pw":
				req.Pw = &v
			}
		}
	}

	err, conn := client.request(req, rpbCounterUpdateReq)
	if err != nil {
		return err
	}
	res := &pb.RpbCounterUpdateResp{}
	return client.response(conn, res)
}

func (client *Client) CounterGet(bucket, key string, options ...map[string]uint32) (int64, error) {
	req := &pb.RpbCounterGetReq{ Bucket: []byte(bucket),
		Key:    []byte(key),
	}
	for _, omap := range options {
		for k, v := range omap {
			switch k {
			case "r":
				req.R = &v
			case "pr":
				req.Pr = &v
			}
		}
	}

	err, conn := client.request(req, rpbCounterGetReq)
	if err != nil {
		return 0, err
	}
	res := &pb.RpbCounterGetResp{}
	err = client.response(conn, res)
	return res.GetValue(), err
}
