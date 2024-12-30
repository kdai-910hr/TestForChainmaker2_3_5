package tbft

import (
	"reflect"
	"testing"

	"chainmaker.org/chainmaker/common/v2/msgbus"
	"chainmaker.org/chainmaker/consensus-utils/v2/consistent_service"
	"chainmaker.org/chainmaker/pb-go/v2/consensus/tbft"
	tbftpb "chainmaker.org/chainmaker/pb-go/v2/consensus/tbft"
	netpb "chainmaker.org/chainmaker/pb-go/v2/net"
	"chainmaker.org/chainmaker/protocol/v2"
	"github.com/stretchr/testify/require"
)

//
//  TestUpdateStatus
//  @Description: TestUpdateStatus
//  @param t
//
func TestUpdateStatus(t *testing.T) {

	rs := &RemoteState{
		Id:               "state.Id",
		Height:           0,
		Round:            0,
		Step:             0,
		Proposal:         nil,
		VerifingProposal: nil,
		LockedRound:      0,
		LockedProposal:   nil,
		ValidRound:       0,
		ValidProposal:    nil,
		RoundVoteSet:     nil,
	}
	rss := make(map[int8]consistent_service.Status)
	rss[rs.Type()] = rs
	remoteInfo := &Node{id: "state.Id", status: rss}
	require.NotNil(t, remoteInfo)
	id := remoteInfo.ID()
	require.NotNil(t, id)
	status, ok := remoteInfo.Statuses()[rs.Type()]
	require.NotNil(t, status)
	require.True(t, ok)
	rs = &RemoteState{
		Id:               "state.Id",
		Height:           1,
		Round:            1,
		Step:             1,
		Proposal:         nil,
		VerifingProposal: nil,
		LockedRound:      0,
		LockedProposal:   nil,
		ValidRound:       0,
		ValidProposal:    nil,
		RoundVoteSet:     nil,
	}
	remoteInfo.UpdateStatus(rs)
}

//
//  TestDecode
//  @Description: TestDecode
//  @param t
//
func TestDecode(t *testing.T) {
	decode := StatusDecoder{log: newMockLogger()}
	prevotes := NewVoteSet(tLogger, tbftpb.VoteType_VOTE_PREVOTE, 1, 0, tbftImpl.validatorSet)
	precommits := NewVoteSet(tLogger, tbftpb.VoteType_VOTE_PRECOMMIT, 1, 0, tbftImpl.validatorSet)
	decode.newRoundVoteSet(newRoundVoteSet(1, 0, prevotes, precommits).ToProto())
	v := decode.Decode(nil)
	require.NotNil(t, v)
	var msg = msgbus.Message{Topic: msgbus.Topic(decode.MsgType())}
	v = decode.Decode(msg)
	require.NotNil(t, v)
	prevotes.Votes["node1"] = nil
	decode.newVoteSet(prevotes.ToProto())
}

//
//  TestDecoder_Decode
//  @Description: TestDecoder_Decode
//  @param t
//
func TestDecoder_Decode(t *testing.T) {

	gossipState := &tbftpb.GossipState{Id: "node1_id", Height: 0, Round: 0}
	tbftMsg := &tbftpb.TBFTMsg{Type: tbftpb.TBFTMsgType_MSG_STATE, Msg: mustMarshal(gossipState)}
	msg := mustMarshal(tbftMsg)

	rs := &RemoteState{
		Id:               "node1_id",
		Height:           0,
		Round:            0,
		Step:             0,
		Proposal:         nil,
		VerifingProposal: nil,
		LockedRound:      0,
		LockedProposal:   nil,
		ValidRound:       0,
		ValidProposal:    nil,
		RoundVoteSet:     nil,
	}
	rss := make(map[int8]consistent_service.Status)
	rss[rs.Type()] = rs
	remoteInfo := &Node{id: "node1_id", status: rss}

	type fields struct {
		log      protocol.Logger
		tbftImpl *ConsensusTBFTImpl
	}
	type args struct {
		d interface{}
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   interface{}
	}{
		{
			"test",
			fields{
				newMockLogger(), tbftImpl,
			},
			args{
				nil,
			},
			"not msgbus.Message",
		},
		{
			"test",
			fields{
				newMockLogger(), tbftImpl,
			},
			args{
				&msgbus.Message{Topic: TypeRemoteTBFTState,
					Payload: netpb.NetMsg{Payload: nil, Type: netpb.NetMsg_CONSISTENT_MSG, To: "node1_id"}},
			},
			"not netpb.NetMsg",
		},
		{
			"test",
			fields{
				newMockLogger(), tbftImpl,
			},
			args{
				&msgbus.Message{Topic: TypeRemoteTBFTState,
					Payload: &netpb.NetMsg{Payload: nil, Type: netpb.NetMsg_CONSISTENT_MSG, To: "node1_id"}},
			},
			"not state message",
		},
		{
			"test",
			fields{
				newMockLogger(), tbftImpl,
			},
			args{
				&msgbus.Message{Topic: TypeRemoteTBFTState,
					Payload: &netpb.NetMsg{Payload: msg, Type: netpb.NetMsg_CONSISTENT_MSG, To: "node1_id"}},
			},
			remoteInfo,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tD := &StatusDecoder{
				log: tt.fields.log, tbftImpl: tt.fields.tbftImpl,
			}
			if got := tD.Decode(tt.args.d); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Decode() = %v, want %v", got, tt.want)
			}
		})
	}
}

//
//  TestDecoder_MsgType
//  @Description: TestDecoder_MsgType
//  @param t
//
func TestDecoder_MsgType(t *testing.T) {
	type fields struct {
		log protocol.Logger
	}
	tests := []struct {
		name   string
		fields fields
		want   int8
	}{
		{"test",
			fields{
				newMockLogger(),
			},
			int8(msgbus.RecvConsistentMsg),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tD := &StatusDecoder{
				log: tt.fields.log,
			}
			if got := tD.MsgType(); got != tt.want {
				t.Errorf("MsgType() = %v, want %v", got, tt.want)
			}
		})
	}
}

//
//  TestNodeInfo_UpdateStatus
//  @Description: TestNodeInfo_UpdateStatus
//  @param t
//
func TestNodeInfo_UpdateStatus(t *testing.T) {

	status := make(map[int8]consistent_service.Status)
	s := &RemoteState{Id: "node1", Height: 1, Round: 1, Step: 1}
	status[s.Type()] = s
	node := &Node{id: "node1", status: status}

	type fields struct {
		id     string
		status map[int8]consistent_service.Status
	}
	type args struct {
		s consistent_service.Status
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{"test",
			fields{node.id, node.status},
			args{
				node.status[s.Type()],
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := &Node{
				id:     tt.fields.id,
				status: tt.fields.status,
			}
			l.UpdateStatus(tt.args.s)
		})
	}
}

//
//  TestRemoteState_Data
//  @Description: TestRemoteState_Data
//  @param t
//
func TestRemoteState_Data(t *testing.T) {

	rs := &RemoteState{
		Id:               "node1",
		Height:           1,
		Round:            1,
		Step:             1,
		Proposal:         nil,
		VerifingProposal: nil,
		LockedRound:      1,
		LockedProposal:   nil,
		ValidRound:       1,
		ValidProposal:    nil,
		RoundVoteSet:     nil,
	}

	type fields struct {
		Id               string
		Height           uint64
		Round            int32
		Step             tbft.Step
		Proposal         []byte
		VerifingProposal []byte
		LockedRound      int32
		LockedProposal   *tbftpb.Proposal
		ValidRound       int32
		ValidProposal    *tbftpb.Proposal
		RoundVoteSet     *roundVoteSet
	}
	tests := []struct {
		name   string
		fields fields
		want   interface{}
	}{
		{"test",
			fields{"node1", 1, 1, 1, nil, nil, 1, nil, 1,
				nil, nil},
			rs},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &RemoteState{
				Id:               tt.fields.Id,
				Height:           tt.fields.Height,
				Round:            tt.fields.Round,
				Step:             tt.fields.Step,
				Proposal:         tt.fields.Proposal,
				VerifingProposal: tt.fields.VerifingProposal,
				LockedRound:      tt.fields.LockedRound,
				LockedProposal:   tt.fields.LockedProposal,
				ValidRound:       tt.fields.ValidRound,
				ValidProposal:    tt.fields.ValidProposal,
				RoundVoteSet:     tt.fields.RoundVoteSet,
			}
			if got := r.Data(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Data() = %v, want %v", got, tt.want)
			}
		})
	}
}
