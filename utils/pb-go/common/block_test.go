/*
 * Copyright (C) BABEC. All rights reserved.
 * Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 *
 */

package common

import (
	"testing"

	"chainmaker.org/chainmaker/pb-go/v2/accesscontrol"
)

func TestBlock_GetTxKey(t *testing.T) {
	b1 := &Block{}
	t.Log(b1.GetTxKey())
	b2 := &Block{Header: &BlockHeader{BlockHeight: 123}}
	t.Log(b2.GetTxKey())
	b2.Header.Proposer = &accesscontrol.Member{MemberInfo: []byte("User1")}
	t.Log(b2.GetTxKey())
}
