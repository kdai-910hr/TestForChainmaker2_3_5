package tbft

import (
	"reflect"
	"testing"

	"chainmaker.org/chainmaker/pb-go/v2/common"
	consensuspb "chainmaker.org/chainmaker/pb-go/v2/consensus"
	"chainmaker.org/chainmaker/pb-go/v2/consensus/tbft"
	tbftpb "chainmaker.org/chainmaker/pb-go/v2/consensus/tbft"
	"chainmaker.org/chainmaker/protocol/v2"
)

func TestNewProposalBlock(t *testing.T) {
	pb := &consensuspb.ProposalBlock{
		Block:    nil,
		TxsRwSet: nil,
	}
	type args struct {
		block    *common.Block
		txsRwSet map[string]*common.TxRWSet
	}
	tests := []struct {
		name string
		args args
		want *consensuspb.ProposalBlock
	}{
		// dpos uses this method, tbft does not
		{"test",
			args{nil, nil},
			pb,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewProposalBlock(tt.args.block, tt.args.txsRwSet); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewProposalBlock() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestVoteSet_String(t *testing.T) {
	type fields struct {
		logger       protocol.Logger
		Type         tbft.VoteType
		Height       uint64
		Round        int32
		Sum          uint64
		Maj23        []byte
		Votes        map[string]*tbftpb.Vote
		VotesByBlock map[string]*BlockVotes
		validators   *validatorSet
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{"test",
			fields{tLogger, 0, 1, 1, 1, nil, nil, nil, nil},
			"{Type: VOTE_PREVOTE, Height: 1, Round: 1, Votes: []}"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vs := &VoteSet{
				logger:       tt.fields.logger,
				Type:         tt.fields.Type,
				Height:       tt.fields.Height,
				Round:        tt.fields.Round,
				Sum:          tt.fields.Sum,
				Maj23:        tt.fields.Maj23,
				Votes:        tt.fields.Votes,
				VotesByBlock: tt.fields.VotesByBlock,
				validators:   tt.fields.validators,
			}
			if got := vs.String(); got != tt.want {
				t.Errorf("String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestVoteSet_AddVoteForConsistent(t *testing.T) {
	var voteHeight uint64 = 2
	var voteRound int32 = 2
	voteHash := []byte("H2R0")
	prevote := NewVote(tbftpb.VoteType_VOTE_PREVOTE, org1NodeId, voteHeight, voteRound, voteHash[:])
	precommit := NewVote(tbftpb.VoteType_VOTE_PRECOMMIT, org1NodeId, voteHeight, voteRound, voteHash[:])
	type fields struct {
		logger       protocol.Logger
		Type         tbftpb.VoteType
		Height       uint64
		Round        int32
		Sum          uint64
		Maj23        []byte
		Votes        map[string]*tbftpb.Vote
		VotesByBlock map[string]*BlockVotes
		validators   *validatorSet
	}
	type args struct {
		vote *tbftpb.Vote
	}
	tests := []struct {
		name      string
		fields    fields
		args      args
		wantAdded bool
		wantErr   bool
	}{
		{"test",
			fields{tLogger, 0, 2, 2, 1, nil, nil, nil, nil},
			args{precommit},
			false,
			true,
		},
		{"test",
			fields{tLogger, 1, 2, 2, 1, nil, nil, nil, nil},
			args{prevote},
			false,
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vs := &VoteSet{
				logger:       tt.fields.logger,
				Type:         tt.fields.Type,
				Height:       tt.fields.Height,
				Round:        tt.fields.Round,
				Sum:          tt.fields.Sum,
				Maj23:        tt.fields.Maj23,
				Votes:        tt.fields.Votes,
				VotesByBlock: tt.fields.VotesByBlock,
				validators:   tt.fields.validators,
			}
			gotAdded, err := vs.AddVoteForConsistent(tt.args.vote)
			if (err != nil) != tt.wantErr {
				t.Errorf("AddVoteForConsistent() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotAdded != tt.wantAdded {
				t.Errorf("AddVoteForConsistent() gotAdded = %v, want %v", gotAdded, tt.wantAdded)
			}
		})
	}
}

func TestVoteSet_HasTwoThirdsMajority(t *testing.T) {
	type fields struct {
		logger       protocol.Logger
		Type         tbftpb.VoteType
		Height       uint64
		Round        int32
		Sum          uint64
		Maj23        []byte
		Votes        map[string]*tbftpb.Vote
		VotesByBlock map[string]*BlockVotes
		validators   *validatorSet
	}
	tests := []struct {
		name         string
		fields       fields
		wantMajority bool
	}{
		{"test",
			fields{tLogger, 0, 1, 1, 1, nil, nil, nil, nil},
			false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vs := &VoteSet{
				logger:       tt.fields.logger,
				Type:         tt.fields.Type,
				Height:       tt.fields.Height,
				Round:        tt.fields.Round,
				Sum:          tt.fields.Sum,
				Maj23:        tt.fields.Maj23,
				Votes:        tt.fields.Votes,
				VotesByBlock: tt.fields.VotesByBlock,
				validators:   tt.fields.validators,
			}
			if gotMajority := vs.HasTwoThirdsMajority(); gotMajority != tt.wantMajority {
				t.Errorf("HasTwoThirdsMajority() = %v, want %v", gotMajority, tt.wantMajority)
			}
		})
	}
}

func TestVoteSet_Size(t *testing.T) {
	type fields struct {
		logger       protocol.Logger
		Type         tbftpb.VoteType
		Height       uint64
		Round        int32
		Sum          uint64
		Maj23        []byte
		Votes        map[string]*tbftpb.Vote
		VotesByBlock map[string]*BlockVotes
		validators   *validatorSet
	}
	tests := []struct {
		name   string
		fields fields
		want   int32
	}{
		{"test",
			fields{tLogger, 0, 1, 1, 1, nil, nil, nil, nil},
			0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vs := &VoteSet{
				logger:       tt.fields.logger,
				Type:         tt.fields.Type,
				Height:       tt.fields.Height,
				Round:        tt.fields.Round,
				Sum:          tt.fields.Sum,
				Maj23:        tt.fields.Maj23,
				Votes:        tt.fields.Votes,
				VotesByBlock: tt.fields.VotesByBlock,
				validators:   tt.fields.validators,
			}
			if got := vs.Size(); got != tt.want {
				t.Errorf("Size() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_roundVoteSet_String(t *testing.T) {
	type fields struct {
		Height     uint64
		Round      int32
		Prevotes   *VoteSet
		Precommits *VoteSet
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{"test",
			fields{0, 0, nil, nil},
			"Height: 0, Round: 0, Prevotes: , Precommits: "},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rvs := &roundVoteSet{
				Height:     tt.fields.Height,
				Round:      tt.fields.Round,
				Prevotes:   tt.fields.Prevotes,
				Precommits: tt.fields.Precommits,
			}
			if got := rvs.String(); got != tt.want {
				t.Errorf("String() = %v, want %v", got, tt.want)
			}
		})
	}
}
