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

func IndexOfInt(element int, data []int) (int) {
	for k, v := range data {
		if element == v {
			return k
		}
	}
	return -1    //not found.
 }