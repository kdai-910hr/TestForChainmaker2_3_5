/*
 * Copyright (C) BABEC. All rights reserved.
 * Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 *
 */

package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetBlockHeaderVersion(t *testing.T) {
	tt := map[string]uint32{
		"v2.2.0":       2201,
		"v2.3.0_alpha": 2300,
		"v2.3.0":       2301,
		"v2.2.2":       2220,
		"v2.0.0":       20,
		"v2.2.0_alpha": 220,
		"2300":         2300,
		"v2.2.3":       2230,
		"2.3.x":        0,
		"2240":         2240,
		"v2.3.1":       2030100,
		"2030200":      2030200,
		"2040200":      2040200,
		"2040000":      2040000,
	}
	for v, result := range tt {
		cc := &ChainConfig{Version: v}
		intV := cc.GetBlockVersion()
		assert.Equal(t, result, intV, v)
	}
}
