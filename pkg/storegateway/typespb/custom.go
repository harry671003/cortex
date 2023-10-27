package typespb

import (
	bytes "bytes"
	"encoding/binary"
	fmt "fmt"
	"sort"
	strconv "strconv"
	strings "strings"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
)

var PartialResponseStrategyValues = func() []string {
	var s []string
	for k := range PartialResponseStrategy_value {
		s = append(s, k)
	}
	sort.Strings(s)
	return s
}()

// Compare returns positive 1 if chunk is smaller -1 if larger than b by min time, then max time.
// It returns 0 if chunks are exactly the same.
func (m AggrChunk) Compare(b AggrChunk) int {
	if m.MinTime < b.MinTime {
		return 1
	}
	if m.MinTime > b.MinTime {
		return -1
	}

	// Same min time.
	if m.MaxTime < b.MaxTime {
		return 1
	}
	if m.MaxTime > b.MaxTime {
		return -1
	}

	// We could use proto.Equal, but we need ordering as well.
	for _, cmp := range []func() int{
		func() int { return m.Raw.Compare(b.Raw) },
		func() int { return m.Count.Compare(b.Count) },
		func() int { return m.Sum.Compare(b.Sum) },
		func() int { return m.Min.Compare(b.Min) },
		func() int { return m.Max.Compare(b.Max) },
		func() int { return m.Counter.Compare(b.Counter) },
	} {
		if c := cmp(); c == 0 {
			continue
		} else {
			return c
		}
	}
	return 0
}

// Compare returns positive 1 if chunk is smaller -1 if larger.
// It returns 0 if chunks are exactly the same.
func (m *Chunk) Compare(b *Chunk) int {
	if m == nil && b == nil {
		return 0
	}
	if b == nil {
		return 1
	}
	if m == nil {
		return -1
	}

	if m.Type < b.Type {
		return 1
	}
	if m.Type > b.Type {
		return -1
	}
	return bytes.Compare(m.Data, b.Data)
}

func (x *PartialResponseStrategy) UnmarshalJSON(entry []byte) error {
	fieldStr, err := strconv.Unquote(string(entry))
	if err != nil {
		return errors.Wrapf(err, fmt.Sprintf("failed to unqote %v, in order to unmarshal as 'partial_response_strategy'. Possible values are %s", string(entry), strings.Join(PartialResponseStrategyValues, ",")))
	}

	if fieldStr == "" {
		// NOTE: For Rule default is abort as this is recommended for alerting.
		*x = ABORT
		return nil
	}

	strategy, ok := PartialResponseStrategy_value[strings.ToUpper(fieldStr)]
	if !ok {
		return errors.Errorf(fmt.Sprintf("failed to unmarshal %v as 'partial_response_strategy'. Possible values are %s", string(entry), strings.Join(PartialResponseStrategyValues, ",")))
	}
	*x = PartialResponseStrategy(strategy)
	return nil
}

func (x *PartialResponseStrategy) MarshalJSON() ([]byte, error) {
	return []byte(strconv.Quote(x.String())), nil
}

// PromMatchersToMatchers returns proto matchers from Prometheus matchers.
// NOTE: It allocates memory.
func PromMatchersToMatchers(ms ...*labels.Matcher) ([]LabelMatcher, error) {
	res := make([]LabelMatcher, 0, len(ms))
	for _, m := range ms {
		var t LabelMatcher_Type

		switch m.Type {
		case labels.MatchEqual:
			t = EQ
		case labels.MatchNotEqual:
			t = NEQ
		case labels.MatchRegexp:
			t = RE
		case labels.MatchNotRegexp:
			t = NRE
		default:
			return nil, errors.Errorf("unrecognized matcher type %d", m.Type)
		}
		res = append(res, LabelMatcher{Type: t, Name: m.Name, Value: m.Value})
	}
	return res, nil
}

// MatchersToPromMatchers returns Prometheus matchers from proto matchers.
// NOTE: It allocates memory.
func MatchersToPromMatchers(ms ...LabelMatcher) ([]*labels.Matcher, error) {
	res := make([]*labels.Matcher, 0, len(ms))
	for _, m := range ms {
		var t labels.MatchType

		switch m.Type {
		case EQ:
			t = labels.MatchEqual
		case NEQ:
			t = labels.MatchNotEqual
		case RE:
			t = labels.MatchRegexp
		case NRE:
			t = labels.MatchNotRegexp
		default:
			return nil, errors.Errorf("unrecognized label matcher type %d", m.Type)
		}
		m, err := labels.NewMatcher(t, m.Name, m.Value)
		if err != nil {
			return nil, err
		}
		res = append(res, m)
	}
	return res, nil
}

// MatchersToString converts label matchers to string format.
// String should be parsable as a valid PromQL query metric selector.
func MatchersToString(ms ...LabelMatcher) string {
	var res string
	for i, m := range ms {
		res += m.PromString()
		if i < len(ms)-1 {
			res += ", "
		}
	}
	return "{" + res + "}"
}

// PromMatchersToString converts prometheus label matchers to string format.
// String should be parsable as a valid PromQL query metric selector.
func PromMatchersToString(ms ...*labels.Matcher) string {
	var res string
	for i, m := range ms {
		res += m.String()
		if i < len(ms)-1 {
			res += ", "
		}
	}
	return "{" + res + "}"
}

func (m *LabelMatcher) PromString() string {
	return fmt.Sprintf("%s%s%q", m.Name, m.Type.PromString(), m.Value)
}

func (x LabelMatcher_Type) PromString() string {
	typeToStr := map[LabelMatcher_Type]string{
		EQ:  "=",
		NEQ: "!=",
		RE:  "=~",
		NRE: "!~",
	}
	if str, ok := typeToStr[x]; ok {
		return str
	}
	panic("unknown match type")
}

// PromLabels return Prometheus labels.Labels without extra allocation.
func (m *Series) PromLabels() labels.Labels {
	return labelpb.ZLabelsToPromLabels(m.Labels)
}

// XORNumSamples return number of samples. Returns 0 if it's not XOR chunk.
func (m *Chunk) XORNumSamples() int {
	if m.Type == XOR {
		return int(binary.BigEndian.Uint16(m.Data))
	}
	return 0
}
