package repositories

type BeforeCreateHook interface {
	BeforeCreate()
}

type BeforeUpdateHook interface {
	BeforeUpdate()
}
