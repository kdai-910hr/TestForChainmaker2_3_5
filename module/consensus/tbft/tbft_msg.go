/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package tbft

import (
	tbftpb "chainmaker.org/chainmaker/pb-go/v2/consensus/tbft"
)

// TBFTProposal represents the marshaled proposal
type TBFTProposal struct {
	PbMsg *tbftpb.Proposal
	// byte format *tbftpb.Proposal
	Bytes []byte
}

// NewTBFTProposal create tbft proposal instance
func NewTBFTProposal(proposal *tbftpb.Proposal, marshal bool) *TBFTProposal {
	tbftProposal := &TBFTProposal{
		PbMsg: proposal,
	}
	if marshal {
		// need marshal
		tbftProposal.Bytes = mustMarshal(proposal)
	}
	return tbftProposal
}

// Marshal marshal the proposal and not care the old bytes
func (p *TBFTProposal) Marshal() {
	p.Bytes = mustMarshal(p.PbMsg)
}
