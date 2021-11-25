package executors

type ErrNoTuple struct{

}

func (e ErrNoTuple) Error() string{
	return "no tuple left"
}