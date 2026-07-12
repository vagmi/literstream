module literstream-fixtures-gen

go 1.24

require github.com/superfly/ltx v0.0.0

require github.com/pierrec/lz4/v4 v4.1.23 // indirect

// The superfly/ltx checkout is symlinked into the repo at references/ltx.
replace github.com/superfly/ltx => ../../../references/ltx
