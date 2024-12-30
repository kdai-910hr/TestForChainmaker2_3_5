/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package tbft

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidatorSetUpdateValidators(t *testing.T) {
	type fields struct {
		Validators []string
	}
	type args struct {
		validators []string
	}
	tests := []struct {
		name                  string
		fields                fields
		args                  args
		wantAddedValidators   []string
		wantRemovedValidators []string
		wantErr               bool
	}{
		{
			"ValidatorSet not change",
			fields{
				[]string{"org1", "org2", "org3", "org4"},
			},
			args{
				[]string{"org1", "org2", "org3", "org4"},
			},
			nil,
			nil,
			false,
		},
		{
			"ValidatorSet add two validators",
			fields{
				[]string{"org1", "org2", "org3", "org4"},
			},
			args{
				[]string{"org1", "org2", "org3", "org4", "org5", "org6"},
			},
			[]string{"org5", "org6"},
			nil,
			false,
		},
		{
			"ValidatorSet remove two validators",
			fields{
				[]string{"org1", "org2", "org3", "org4"},
			},
			args{
				[]string{"org1", "org2"},
			},
			nil,
			[]string{"org3", "org4"},
			false,
		},
		{
			"ValidatorSet replace two validators",
			fields{
				[]string{"org1", "org2", "org3", "org4"},
			},
			args{
				[]string{"org1", "org2", "org5", "org6"},
			},
			[]string{"org5", "org6"},
			[]string{"org3", "org4"},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			valSet := &validatorSet{
				logger:           cmLogger,
				Validators:       tt.fields.Validators,
				validatorsHeight: make(map[string]uint64),
			}
			gotAddedValidators, gotRemovedValidators, err := valSet.updateValidators(tt.args.validators)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidatorSet.updateValidators() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotAddedValidators, tt.wantAddedValidators) {
				t.Errorf("ValidatorSet.updateValidators() gotAddedValidators = %v, want %v", gotAddedValidators, tt.wantAddedValidators)
			}
			if !reflect.DeepEqual(gotRemovedValidators, tt.wantRemovedValidators) {
				t.Errorf("ValidatorSet.updateValidators() gotRemovedValidators = %v, want %v", gotRemovedValidators, tt.wantRemovedValidators)
			}
			if valSet.String() == "" {
				t.Errorf("ValidatorSet.String() error , string is nil ")
			}
		})
	}
}

func TestValidatorGetProposer(t *testing.T) {
	var valSet *validatorSet
	_, err := valSet.GetProposer(blockVersion231, "", 1, 0)
	require.Equal(t, err, ErrInvalidIndex)
	valSet = &validatorSet{
		logger:            cmLogger,
		Validators:        []string{"org1", "org2", "org3", "org4"},
		validatorsHeight:  make(map[string]uint64),
		blocksPerProposer: 10,
	}

	index := valSet.getIndexByString("org2")
	require.Equal(t, index, int32(1))
	index = valSet.getIndexByString("nil")
	require.Equal(t, index, int32(0))

	v, err := valSet.GetProposer(blockVersion231, "org2", 1, 0)
	require.Equal(t, err, nil)
	require.Equal(t, v, "org2")
}
