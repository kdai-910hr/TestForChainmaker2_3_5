/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package tbft

import (
	"errors"
	"fmt"
	"sort"
	"sync"

	"chainmaker.org/chainmaker/protocol/v2"
)

var (
	// ErrInvalidIndex implements the error for invalid index of validators
	ErrInvalidIndex = errors.New("invalid index")
)

//
// validatorSet
// @Description: validator set
//
type validatorSet struct {
	sync.Mutex
	logger     protocol.Logger
	Validators []string
	// Validator's current block height
	validatorsHeight map[string]uint64
	// Validator's beat Time
	validatorsBeatTime map[string]int64
	// The number of consecutive proposals by the proposer
	blocksPerProposer uint64
}

//
// newValidatorSet
// @Description: create a new validator set
// @param logger
// @param validators
// @param blocksPerProposer
// @return *validatorSet
//
func newValidatorSet(logger protocol.Logger, validators []string, blocksPerProposer uint64) *validatorSet {
	sort.SliceStable(validators, func(i, j int) bool {
		return validators[i] < validators[j]
	})

	valSet := &validatorSet{
		logger:             logger,
		Validators:         validators,
		validatorsHeight:   make(map[string]uint64),
		validatorsBeatTime: make(map[string]int64),
		blocksPerProposer:  blocksPerProposer,
	}
	valSet.logger.Infof("new validator set: %v", validators)

	return valSet
}

//
// isNilOrEmpty
// @Description: when the validatorSet is nil or empty return true
// @receiver valSet
// @return bool
//
func (valSet *validatorSet) isNilOrEmpty() bool {
	if valSet == nil {
		return true
	}
	valSet.Lock()
	defer valSet.Unlock()
	return len(valSet.Validators) == 0
}

//
// String
// @Description: convert *validatorSet to string
// @receiver valSet
// @return string
//
func (valSet *validatorSet) String() string {
	if valSet == nil {
		return ""
	}
	valSet.Lock()
	defer valSet.Unlock()

	return fmt.Sprintf("%v", valSet.Validators)

}

//
// Size
// @Description: get validatorSet size
// @receiver valSet
// @return int32
//
func (valSet *validatorSet) Size() int32 {
	if valSet == nil {
		return 0
	}

	valSet.Lock()
	defer valSet.Unlock()

	return int32(len(valSet.Validators))
}

// HasValidator holds the lock and return whether validator is in
// the validatorSet
func (valSet *validatorSet) HasValidator(validator string) bool {
	if valSet == nil {
		return false
	}

	valSet.Lock()
	defer valSet.Unlock()

	return valSet.hasValidator(validator)
}

func (valSet *validatorSet) hasValidator(validator string) bool {
	for _, val := range valSet.Validators {
		if val == validator {
			return true
		}
	}
	return false
}

// GetProposerV230 calculate proposer based on height and round in versions earlier than v2.3.1
// we used this function in versions prior to v2.3.1 for version compatibility and node upgrades
func (valSet *validatorSet) GetProposerV230(height uint64, round int32) (validator string, err error) {
	if valSet.isNilOrEmpty() {
		return "", ErrInvalidIndex
	}

	heightOffset := int32((height + 1) / valSet.blocksPerProposer)
	roundOffset := round % valSet.Size()
	proposerIndex := (heightOffset + roundOffset) % valSet.Size()

	return valSet.getByIndex(proposerIndex)
}

//
// GetProposer
// @Description:Calculate proposer based on height and round
// @receiver valSet
// @param height
// @param round
// @return validator
// @return err
//
func (valSet *validatorSet) GetProposer(blockVersion uint32, preProposer string,
	height uint64, round int32) (validator string, err error) {
	if blockVersion < blockVersion231 {
		return valSet.GetProposerV230(height, round)
	}
	if valSet.isNilOrEmpty() {
		return "", ErrInvalidIndex
	}

	proposerOffset := valSet.getIndexByString(preProposer)
	if (height % valSet.blocksPerProposer) == 0 {
		proposerOffset++
	}
	roundOffset := round % valSet.Size()
	proposerIndex := (roundOffset + proposerOffset) % valSet.Size()

	return valSet.getByIndex(proposerIndex)
}

//
// updateValidators
// @Description: Update the collection based on the input and return an array of additions and deletions
// @receiver valSet
// @param validators
// @return addedValidators
// @return removedValidators
// @return err
//
func (valSet *validatorSet) updateValidators(validators []string) (addedValidators []string, removedValidators []string,
	err error) {
	valSet.Lock()
	defer valSet.Unlock()

	removedValidatorsMap := make(map[string]bool)
	for _, v := range valSet.Validators {
		removedValidatorsMap[v] = true
	}

	for _, v := range validators {
		// addedValidators
		if !valSet.hasValidator(v) {
			addedValidators = append(addedValidators, v)
		}

		delete(removedValidatorsMap, v)
	}

	// removedValidators
	for k := range removedValidatorsMap {
		removedValidators = append(removedValidators, k)
	}

	sort.SliceStable(validators, func(i, j int) bool {
		return validators[i] < validators[j]
	})

	valSet.Validators = validators

	sort.SliceStable(addedValidators, func(i, j int) bool {
		return addedValidators[i] < addedValidators[j]
	})

	sort.SliceStable(removedValidators, func(i, j int) bool {
		return removedValidators[i] < removedValidators[j]
	})
	valSet.logger.Infof("%v update validators, validators: %v, addedValidators: %v, removedValidators: %v",
		valSet.Validators, validators, addedValidators, removedValidators)
	return
}

//
// updateBlocksPerProposer
// @Description: Update the number of consecutive blocks produced by the proposer
// @receiver valSet
// @param blocks
// @return error
//
func (valSet *validatorSet) updateBlocksPerProposer(blocks uint64) error {
	valSet.Lock()
	defer valSet.Unlock()

	valSet.blocksPerProposer = blocks

	return nil
}

//
// getByIndex
// @Description: Get proposer by index
// @receiver valSet
// @param index
// @return validator
// @return err
//
func (valSet *validatorSet) getByIndex(index int32) (validator string, err error) {
	if index < 0 || index >= valSet.Size() {
		return "", ErrInvalidIndex
	}

	valSet.Lock()
	defer valSet.Unlock()

	val := valSet.Validators[index]
	return val, nil
}

// getIndexByString
// @Description: Get index of validator
// @receiver valSet
// @param preProposer
// @return index
// @return int32
//
func (valSet *validatorSet) getIndexByString(preProposer string) int32 {
	for i, validator := range valSet.Validators {
		if validator == preProposer {
			return int32(i)
		}
	}
	return 0
}

// checkProposed checks whether the local node can generate the proposal.
// if there are f+1 remote nodes whose height is greater than that of the
// local node, the local node does not need to generate the proposal.
// return true indicates that a proposal can be generated.
func (valSet *validatorSet) checkProposed(height uint64) bool {
	if valSet == nil {
		return false
	}

	valSet.Lock()
	defer valSet.Unlock()

	quorum := 0
	for _, v := range valSet.Validators {
		validatorHeight := valSet.validatorsHeight[v]
		if validatorHeight > height {
			quorum++
		}
		// f+1 remote nodes are higher than the local nodes
		if quorum >= len(valSet.Validators)/3+1 {
			valSet.logger.Infof("The status of the local node is backward "+
				"and no proposal is required, local height = %d", height)
			return false
		}
	}
	return true
}
