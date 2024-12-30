package accesscontrol

func (m *Member) ChangeToMemberFull() *MemberFull {
	return &MemberFull{
		OrgId:      m.OrgId,
		MemberType: m.MemberType,
		MemberInfo: m.MemberInfo,
	}
}

func (m *MemberFull) ChangeToMember() *Member {
	return &Member{
		OrgId:      m.OrgId,
		MemberType: m.MemberType,
		MemberInfo: m.MemberInfo,
	}
}
