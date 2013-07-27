package riak

import (
	"strconv"
	"time"

	"testing"
)

func TestCounter(t *testing.T) {
	client := setupConnection(t)
	bucket, err := client.Bucket("counter-test")
	if err != nil {
		t.Fatal("unexpected err:", err)
	}
	err = bucket.SetAllowMult(true)
	if err != nil {
		t.Fatal("unexpected err:", err)
	}
	key := strconv.FormatInt(time.Now().Unix(), 10)
	t.Log("counter key", key)

	err = client.CounterUpdate("counter-test", key, 10)
	if err != nil {
		t.Fatal("unexpected err:", err)
	}
	err = client.CounterUpdate("counter-test", key, 1)
	if err != nil {
		t.Fatal("unexpected err:", err)
	}

	val, err := client.CounterGet("counter-test", key)
	if err != nil {
		t.Fatal("unexpected err:", err)
	}
	if val != 11 {
		t.Fatal("bad value: want 11, got", val)
	}
}
