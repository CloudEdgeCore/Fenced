package main

import (
	_ "embed"
)

// embeddedDashboardHTML is the console document. It lives in an asset
// file so the markup, the stylesheet and the script stay editable and
// reviewable as web assets instead of a Go string literal.
//
//go:embed ui_assets/dashboard.html
var embeddedDashboardHTML string
