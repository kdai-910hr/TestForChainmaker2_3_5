/*
 * Copyright (C) BABEC. All rights reserved.
 * Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package common

import (
	"crypto/sha256"
)

//GetSenderAccountId 获得交易的发起人的唯一账户标识，这个标识如果大于200字节，则返回的是SHA256 Hash
func (t *Transaction) GetSenderAccountId() []byte {
	var accountId []byte
	if t != nil && t.Sender != nil {
		accountId = t.Sender.Signer.MemberInfo
	}
	if len(accountId) > 200 {
		hash := sha256.Sum256(accountId)
		accountId = hash[:]
	}
	return accountId
}
