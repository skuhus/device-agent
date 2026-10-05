package wire

import (
	"reflect"
	"strings"
	"testing"
)

// Every discard reason and every error class has a counter of its own, named
// as the reason or class is. A value that fell through to another's counter
// would make the unmatched-separator signature unreadable, and one missing
// from its list would never be counted.
func TestEveryReasonAndClassHasItsOwnCounter(t *testing.T) {
	var discards DiscardCounts
	for _, reason := range DiscardReasons {
		discards.Add(reason)
	}
	requireOneInEveryCounter(t, discards, func(name string) bool { return DiscardReason(name).Known() })

	var opens OpenFailureCounts
	for _, class := range ErrorClasses {
		opens.Add(class)
	}
	requireOneInEveryCounter(t, opens, func(name string) bool { return ErrorClass(name).Known() })
}

// requireOneInEveryCounter checks that each counter field holds 1, and that
// its JSON name is a known reason or class.
func requireOneInEveryCounter(t *testing.T, counts any, known func(string) bool) {
	t.Helper()
	value := reflect.ValueOf(counts)
	for index := 0; index < value.NumField(); index++ {
		field := value.Type().Field(index)
		name, _, _ := strings.Cut(field.Tag.Get("json"), ",")
		if got := value.Field(index).Uint(); got != 1 {
			t.Errorf("%T.%s = %d after one of each, want 1", counts, field.Name, got)
		}
		if !known(name) {
			t.Errorf("%T counts %q, which is not in its list", counts, name)
		}
	}
}

// A class nothing defines is counted as unknown; a reason nothing defines is
// not counted at all, since discards has no unknown counter.
func TestValuesWithoutACounter(t *testing.T) {
	var opens OpenFailureCounts
	opens.Add(ErrorClass("gremlins"))
	if opens != (OpenFailureCounts{Unknown: 1}) {
		t.Errorf("an undefined class counted as %+v, want unknown", opens)
	}
	var discards DiscardCounts
	discards.Add(DiscardReason("gremlins"))
	if discards != (DiscardCounts{}) {
		t.Errorf("an undefined reason counted as %+v, want nothing", discards)
	}
}
