/*
 * Copyright (C) BABEC. All rights reserved.
 * Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package common

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

//func TestTxSignature_Unmarshal(t *testing.T) {
//	signData := []byte("Signature")
//	txSign := &TxSignature{}
//	err := txSign.Unmarshal2(signData)
//	t.Log("err:", err)
//	t.Logf("%+v", txSign)
//}
//func TestTxSignature_MultiSig(t *testing.T) {
//	endorsment1 := &EndorsementEntry{
//		Signer:    &accesscontrol.SerializedMember{MemberInfo: []byte("User1")},
//		Signature: []byte("sign1"),
//	}
//	endorsment2 := &EndorsementEntry{
//		Signer:    &accesscontrol.SerializedMember{MemberInfo: []byte("User2")},
//		Signature: []byte("sign2"),
//	}
//	sign := &TxSignature{Endorsement: []*EndorsementEntry{endorsment1, endorsment2}}
//	t.Logf("%+v", sign)
//	data, _ := sign.Marshal()
//	t.Logf("%x", data)
//}
func TestKv(t *testing.T) {
	kv := &KeyValuePair{Key: "key1", Value: []byte("value1")}
	jkv, _ := json.Marshal(kv)
	t.Log(string(jkv))
}
func TestKvBytes(t *testing.T) {
	v := [300]byte{}
	for i := 0; i < 256; i++ {
		v[i] = byte(i)
	}
	kv := &KeyValuePair{Key: "key1", Value: v[:]}
	t.Logf("Key:%s,Value:%x", kv.Key, kv.Value)

	data, _ := json.Marshal(kv)
	t.Log(string(data))
	kv2 := &KeyValuePair{}
	json.Unmarshal(data, &kv2)
	t.Logf("Key:%s,Value:%x", kv2.Key, kv2.Value)
	assert.EqualValues(t, kv.Value, kv2.Value)
}
