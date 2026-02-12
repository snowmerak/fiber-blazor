package statics

import "embed"

//go:embed htmx.min.js
var HtmxJS embed.FS

func GetHtmxJS() ([]byte, error) {
	return HtmxJS.ReadFile("htmx.min.js")
}
