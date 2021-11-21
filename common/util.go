package common

func PanicIfErr(err error) {
	if err != nil {
		panic(err)
	}
}

// Contains tells whether arr contains x.
func Contains(arr []int, x int) bool {
	for _, n := range arr {
		if x == n {
			return true
		}
	}
	return false
}
