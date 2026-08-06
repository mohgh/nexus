// Package protobuf provides hand-written encoding/decoding for the Event message.
//
// Ch05 teaching point: we skip the protoc code-generator here so students
// can see the wire format directly. In production, you would use protoc +
// the generated code. The encoding is identical — only the boilerplate differs.
//
// Proto3 field numbers used (kept stable across schema versions):
//
//	1  = id          (string)
//	2  = tenant_id   (string)
//	3  = event_type  (string)
//	4  = payload     (bytes  — JSON-encoded)
//	5  = value       (double)
//	6  = occurred_at (int64  — Unix nanoseconds)
//
// Adding a new optional field (e.g. field 7 = source) is a backward-compatible
// change — old consumers ignore unknown field numbers. Removing or renumbering
// a field is a BREAKING change that violates the contract with consumers.
package protobuf

import (
	"encoding/binary"
	"fmt"
	"math"
	"time"

	"github.com/mohgh/nexus/internal/domain"
)

// Marshal encodes an Event into a minimal proto3-compatible binary format.
func Marshal(e *domain.Event) ([]byte, error) {
	var b []byte
	b = appendString(b, 1, e.ID)
	b = appendString(b, 2, e.TenantID)
	b = appendString(b, 3, e.EventType)
	b = appendBytes(b, 4, []byte(e.Payload))
	b = appendFloat64(b, 5, e.Value)
	b = appendInt64(b, 6, e.OccurredAt.UnixNano())
	return b, nil
}

// Unmarshal decodes a proto3-compatible binary blob into an Event.
// Unknown field numbers are silently skipped — forward-compatibility.
func Unmarshal(data []byte, e *domain.Event) error {
	for len(data) > 0 {
		fieldNum, wireType, n, err := readTag(data)
		if err != nil {
			return fmt.Errorf("proto: read tag: %w", err)
		}
		data = data[n:]

		switch wireType {
		case 0: // varint
			v, n := binary.Uvarint(data)
			if n <= 0 {
				return fmt.Errorf("proto: bad varint")
			}
			if fieldNum == 6 {
				e.OccurredAt = time.Unix(0, int64(v)).UTC()
			}
			data = data[n:]

		case 1: // 64-bit fixed
			if len(data) < 8 {
				return fmt.Errorf("proto: short 64-bit field")
			}
			bits := binary.LittleEndian.Uint64(data[:8])
			if fieldNum == 5 {
				e.Value = math.Float64frombits(bits)
			}
			data = data[8:]

		case 2: // length-delimited (string / bytes)
			length, n := binary.Uvarint(data)
			if n <= 0 {
				return fmt.Errorf("proto: bad length varint")
			}
			data = data[n:]
			if uint64(len(data)) < length {
				return fmt.Errorf("proto: truncated field %d", fieldNum)
			}
			val := data[:length]
			switch fieldNum {
			case 1:
				e.ID = string(val)
			case 2:
				e.TenantID = string(val)
			case 3:
				e.EventType = string(val)
			case 4:
				e.Payload = append([]byte(nil), val...)
			}
			// Unknown length-delimited fields fall through — the
			// bytes are skipped by the data = data[length:] below.
			data = data[length:]

		case 5: // 32-bit fixed
			// Forward-compat: a future producer adding a float32 or
			// fixed32 field would emit wire type 5. We don't decode
			// any 32-bit fields today, but we must SKIP the 4 bytes
			// cleanly rather than erroring — otherwise a v1 decoder
			// breaks on v2 bytes, defeating the whole point of
			// length-prefixed wire formats.
			if len(data) < 4 {
				return fmt.Errorf("proto: short 32-bit field %d", fieldNum)
			}
			data = data[4:]

		default:
			// Wire types 3 and 4 are deprecated group markers that
			// proto3 doesn't emit. Anything else is a malformed
			// message. Erroring here is correct — silently skipping
			// would mask real corruption.
			return fmt.Errorf("proto: unknown wire type %d for field %d", wireType, fieldNum)
		}
	}
	return nil
}

// ── helpers ───────────────────────────────────────────────────────────────────

func appendTag(b []byte, field, wireType int) []byte {
	return appendVarint(b, uint64(field<<3|wireType))
}

func appendVarint(b []byte, v uint64) []byte {
	var tmp [10]byte
	n := binary.PutUvarint(tmp[:], v)
	return append(b, tmp[:n]...)
}

func appendString(b []byte, field int, s string) []byte {
	return appendBytes(b, field, []byte(s))
}

func appendBytes(b []byte, field int, v []byte) []byte {
	b = appendTag(b, field, 2)
	b = appendVarint(b, uint64(len(v)))
	return append(b, v...)
}

func appendFloat64(b []byte, field int, f float64) []byte {
	b = appendTag(b, field, 1)
	var tmp [8]byte
	binary.LittleEndian.PutUint64(tmp[:], math.Float64bits(f))
	return append(b, tmp[:]...)
}

func appendInt64(b []byte, field int, v int64) []byte {
	b = appendTag(b, field, 0)
	return appendVarint(b, uint64(v))
}

func readTag(data []byte) (fieldNum, wireType, n int, err error) {
	v, n := binary.Uvarint(data)
	if n <= 0 {
		return 0, 0, 0, fmt.Errorf("proto: bad tag varint")
	}
	return int(v >> 3), int(v & 0x7), n, nil
}
