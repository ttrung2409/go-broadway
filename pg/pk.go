package pg

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// UUID is a 128-bit universally unique identifier.
type UUID [16]byte

type PK struct {
	value any // nil | int64 | UUID
}

func pkAt[T int64 | UUID](v T) PK { return PK{value: v} }

func pkFromValue(v any) PK {
	switch x := v.(type) {
	case int64:
		return pkAt(x)
	case int32:
		return pkAt(int64(x))
	case [16]byte:
		return pkAt(UUID(x)) // pgx returns [16]byte for uuid columns
	case string:
		if n, err := strconv.ParseInt(x, 10, 64); err == nil {
			return pkAt(n)
		}
		if u, err := uuidFromString(x); err == nil {
			return pkAt(u)
		}
		return PK{}
	default:
		return PK{}
	}
}

func (p PK) IsSet() bool { return p.value != nil }
func (p PK) Value() any  { return p.value }

func (p PK) Next(n int) PK {
	v, ok := p.value.(int64)
	if !ok {
		return p // UUID: no meaningful increment
	}
	return pkAt(v + int64(n))
}

// inRange reports whether p falls within (low, high].
// An unset low means unbounded start; an unset high means unbounded end.
func (p PK) inRange(low, high PK) bool {
	switch v := p.value.(type) {
	case int64:
		return (!low.IsSet() || v > low.value.(int64)) &&
			(!high.IsSet() || v <= high.value.(int64))
	case UUID:
		return (!low.IsSet() || uuidCmp(v, low.value.(UUID)) > 0) &&
			(!high.IsSet() || uuidCmp(v, high.value.(UUID)) <= 0)
	default:
		return false
	}
}

func (p PK) MarshalJSON() ([]byte, error) {
	switch v := p.value.(type) {
	case int64:
		return json.Marshal(v)
	case UUID:
		return json.Marshal(uuidToString(v))
	default:
		return []byte("null"), nil
	}
}

func (p *PK) UnmarshalJSON(b []byte) error {
	if string(b) == "null" {
		p.value = nil
		return nil
	}
	var n int64
	if err := json.Unmarshal(b, &n); err == nil {
		p.value = n
		return nil
	}
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	u, err := uuidFromString(s)
	if err != nil {
		return err
	}
	p.value = u
	return nil
}

func uuidCmp(a, b UUID) int {
	for i := range a {
		if a[i] != b[i] {
			if a[i] < b[i] {
				return -1
			}
			return 1
		}
	}
	return 0
}

func uuidToString(u UUID) string {
	b := make([]byte, 36)
	hex.Encode(b[0:8], u[0:4])
	b[8] = '-'
	hex.Encode(b[9:13], u[4:6])
	b[13] = '-'
	hex.Encode(b[14:18], u[6:8])
	b[18] = '-'
	hex.Encode(b[19:23], u[8:10])
	b[23] = '-'
	hex.Encode(b[24:36], u[10:16])
	return string(b)
}

func uuidFromString(s string) (UUID, error) {
	var u UUID
	cleaned := strings.ReplaceAll(s, "-", "")
	if len(cleaned) != 32 {
		return u, fmt.Errorf("invalid UUID: %q", s)
	}
	b, err := hex.DecodeString(cleaned)
	if err != nil {
		return u, err
	}
	copy(u[:], b)
	return u, nil
}
