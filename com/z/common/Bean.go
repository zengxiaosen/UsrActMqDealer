package common

type UsrActionRecord struct {
	U         string
	T         string
	Contentid string
	Count     int64 // 1 代表一条记录
}

type UsrClickShowRecord struct {
	U          string
	ClickCount int64
	ShowCount  int64
}

type VVRecord struct {
	U string
	Contid string
	Tm float64
	Duration float64
	Rpt float64
}

type VVSinkURecord struct {
	U string
	Tm float64
	Duration float64
	Rpt float64
}
