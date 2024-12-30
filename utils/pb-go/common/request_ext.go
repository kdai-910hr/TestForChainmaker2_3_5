/*
 * Copyright (C) BABEC. All rights reserved.
 * Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package common

//GetParameter 获得某个参数的值
func (t *Payload) GetParameter(key string) []byte {
	for _, kv := range t.Parameters {
		if kv.Key == key {
			return kv.Value
		}
	}
	return nil
}

//type keyStringValue struct {
//	Key   string `json:"key,omitempty"`
//	Value string `json:"value,omitempty"`
//}
//
//func (kv *KeyValuePair) UnmarshalJSON(data []byte) error {
//	kv2 := keyStringValue{}
//	err := json.Unmarshal(data, &kv2)
//	if err != nil {
//		return err
//	}
//	kv.Key = kv2.Key
//	kv.Value = []byte(kv2.Value)
//	return nil
//}
//func (kv *KeyValuePair) MarshalJSON() ([]byte, error) {
//	kv2 := keyStringValue{Key: kv.Key, Value: string(kv.Value)}
//	return json.Marshal(kv2)
//}
