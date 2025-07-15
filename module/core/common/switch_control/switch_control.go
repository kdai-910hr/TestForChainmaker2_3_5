package switch_control

import (
	"chainmaker.org/chainmaker/protocol/v2"
	"os"
	"strings"
)

type ControlType int

// 当前支持的控制类型
const (
	DEFAULTControl ControlType = iota
	BatchControl
	PartDAGControl
	StaleControl
)

var allControlType = []ControlType{
	DEFAULTControl,
	BatchControl,
	PartDAGControl,
	StaleControl,
}

var constructorMap = map[ControlType]func(protocol.Logger) SwitchController{
	BatchControl:   NewBatchController,
	PartDAGControl: NewPartDAGController,
	StaleControl:   NewStaleController,
}

type SwitchController interface {
	IsEnabled() bool
	TryEnable(_value bool)
}

type StaleControllerImpl struct {
	envSet bool
	log    protocol.Logger
}

func (s *StaleControllerImpl) IsEnabled() bool {
	s.envSet = strings.ToLower(os.Getenv("STALE_READ_PROTECTION_ENABLED")) == "true"
	return s.envSet
}

func (s *StaleControllerImpl) TryEnable(_value bool) {
	var err error
	if _value && !s.envSet {
		err = os.Setenv("STALE_READ_ENABLED", "true")
	} else if !s.envSet {
		err = os.Setenv("STALE_READ_ENABLED", "false")
	}
	if err != nil {
		s.log.Errorf("Failed to set STALE_READ_ENABLED environment variable: %v", err)
	}
	return
}

func NewStaleController(log protocol.Logger) SwitchController {
	return &StaleControllerImpl{
		envSet: false,
		log:    log,
	}
}

type BatchControllerImpl struct {
	log protocol.Logger
}

func (b *BatchControllerImpl) IsEnabled() bool {
	return strings.ToLower(os.Getenv("BATCH_CONTROL_ENABLED")) == "true"
}

func (b *BatchControllerImpl) TryEnable(_value bool) {
	var err error
	if _value {
		err = os.Setenv("BATCH_CONTROL_ENABLED", "true")
	} else {
		err = os.Setenv("BATCH_CONTROL_ENABLED", "false")
	}
	if err != nil {
		b.log.Errorf("Failed to set BATCH_CONTROL_ENABLED environment variable: %v", err)
	}
	return
}

func NewBatchController(log protocol.Logger) SwitchController {
	return &BatchControllerImpl{
		log: log,
	}
}

type PartDAGControllerImpl struct {
	log protocol.Logger
}

func (p *PartDAGControllerImpl) IsEnabled() bool {
	return strings.ToLower(os.Getenv("PART_DAG_CONTROL_ENABLED")) == "true"
}

func (p *PartDAGControllerImpl) TryEnable(_value bool) {
	var err error
	if _value {
		err = os.Setenv("PART_DAG_CONTROL_ENABLED", "true")
	} else {
		err = os.Setenv("PART_DAG_CONTROL_ENABLED", "false")
	}
	if err != nil {
		p.log.Errorf("Failed to set PART_DAG_CONTROL_ENABLED environment variable: %v", err)
	}
	return
}

func NewPartDAGController(log protocol.Logger) SwitchController {
	return &PartDAGControllerImpl{
		log: log,
	}
}

type SwitchControllerImpl struct {
	Controllers map[ControlType]SwitchController
}

func NewSwitchControllerImpl(log protocol.Logger) SwitchControllerImpl {
	ret := SwitchControllerImpl{
		Controllers: make(map[ControlType]SwitchController),
	}
	for _, controlType := range allControlType {
		if constructor, exists := constructorMap[controlType]; exists {
			ret.Controllers[controlType] = constructor(log)
		}
	}
	return ret
}

func (s *SwitchControllerImpl) IsEnabled(controlType ControlType) bool {
	if controller, exists := s.Controllers[controlType]; exists {
		return controller.IsEnabled()
	}
	// 找不到就是什么控制都没切换，就是原生DAG调度
	return controlType == DEFAULTControl
}

func (s *SwitchControllerImpl) TryEnable(controlType ControlType, _value bool) {
	// 原生控制就是所有切换方案都不启用
	for _type, controller := range s.Controllers {
		if _type == controlType {
			controller.TryEnable(_value)
		} else {
			controller.TryEnable(false) // Disable other controllers
		}
	}
}
