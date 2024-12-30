/*
 * Copyright (C) BABEC. All rights reserved.
 * Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 *
 */

package config

import (
	"strconv"
	"strings"
)

//GetBlockVersion get uint32 block version from ChainConfig
func (c *ChainConfig) GetBlockVersion() uint32 {
	if len(c.Version) == 0 {
		return 0
	}
	if c.Version[0] == 'v' {
		return getBlockHeaderVersion(c.Version)
	}
	v, err := strconv.Atoi(c.Version)
	if err != nil {
		return 0
	}
	return uint32(v)
}
func getBlockHeaderVersion(cfgVersion string) uint32 {
	if version, ok := specialVersionMapping[cfgVersion]; ok {
		return version
	}
	// must not is number
	//if num, err := strconv.Atoi(cfgVersion); err == nil {
	//	return uint32(num)
	//}
	if cfgVersion > "v2.2.0" {
		version := string(cfgVersion[1]) + string(cfgVersion[3]) + string(cfgVersion[5])
		if strings.HasSuffix(cfgVersion, ".0") {
			//用于正式版发布的时候，应该是xxx1
			version += "1"
		} else {
			//用于v2.2.0_alpha或者是v2.3.1这样的版本
			version += "0"
		}

		v, err := strconv.Atoi(version)
		if err != nil {
			panic(err)
		}
		return uint32(v)
	}
	return 20
}

//一些特殊的版本映射关系
var specialVersionMapping = map[string]uint32{
	"v2.2.0_alpha": 220,
	"v2.2.0":       2201,
	"v2.3.1":       2030100,
}
