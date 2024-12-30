/*
 * Copyright (C) BABEC. All rights reserved.
 * Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package common

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"time"
)

func (b *Block) Hash() []byte {
	return b.Header.BlockHash
}
func (b *Block) GetBlockHashStr() string {
	return hex.EncodeToString(b.Header.BlockHash)
}

func (b *Block) GetTimestamp() time.Time {
	return time.Unix(b.Header.BlockTimestamp, 0)
}

// GetTxKey get transaction key
func (b *Block) GetTxKey() string {
	if b.Header == nil {
		return "Empty"
	}
	if b.Header.Proposer == nil {
		return fmt.Sprintf("Block[%d-%x]", b.Header.BlockHeight, b.Header.BlockHash)
	}
	return GetTxKeyWith(b.Header.Proposer.MemberInfo, b.Header.BlockHeight)
}

func GetTxKeyWith(propose []byte, blockHeight uint64) string {
	if propose == nil {
		propose = make([]byte, 0)
	}
	f := sha256.New()
	f.Write(propose)
	f.Write([]byte(strconv.Itoa(int(blockHeight))))
	return hex.EncodeToString(f.Sum(nil))
}
