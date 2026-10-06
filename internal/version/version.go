// Package version carries the build identity: a version written in source, and
// a commit and build date injected by the linker. Every message the agent
// publishes and every log line report the version, so a fleet can be asked
// which stations are still running an old build.
package version

// version is the agent's version, reported as agent_version in every message
// the agent publishes (protocol/messages.schema.json), and by the version
// subcommand. This is the only place it is written down: edit it, then tag.
//
// It is a constant rather than a value injected by the linker so that every way
// of building this code agrees: "go build", "go test", an IDE and the Makefile
// all report the same version, and there is no way to produce a binary whose
// agent_version field is a lie about which source it came from.
const version = "2.1.0"

// Commit and date are the one part of the identity source cannot know, so they
// are injected from cmd/skuhus-device-agent via Set, out of
// -ldflags -X main.commit / main.date.
var (
	commit = "none"
	date   = "unknown"
)

// Set records the linker-injected build facts. Empty arguments are ignored so
// that a development build keeps its placeholder values.
func Set(buildCommit, buildDate string) {
	if buildCommit != "" {
		commit = buildCommit
	}
	if buildDate != "" {
		date = buildDate
	}
}

// Version is the agent version reported in the agent_version field.
func Version() string { return version }

// Commit is the full git commit the binary was built from.
func Commit() string { return commit }

// Date is the build timestamp.
func Date() string { return date }

// String renders the full build identity for the version subcommand.
func String() string {
	return "skuhus-device-agent " + version + " commit=" + commit + " built=" + date
}
