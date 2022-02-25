package kafka

import (
	"encoding/json"
	"fmt"
	"time"
)

const (
	//CompressionNone no compression
	CompressionNone CompressionCodec = iota
	//CompressionGZIP compression using GZIP
	CompressionGZIP
	//CompressionSnappy compression using snappy
	CompressionSnappy
	//CompressionLZ4 compression using LZ4
	CompressionLZ4
	//CompressionZSTD compression using ZSTD
	CompressionZSTD

	// The lowest 3 bits contain the compression codec used for the message
	compressionCodecMask int8 = 0x07

	// Bit 3 set for "LogAppend" timestamps
	timestampTypeMask = 0x08

	// CompressionLevelDefault is the constant to use in CompressionLevel
	// to have the default compression level for any codec. The value is picked
	// that we don't use any existing compression levels.
	CompressionLevelDefault = -1000
)

// CompressionCodec represents the various compression codecs recognized by Kafka in messages.
type CompressionCodec int8

// MarshalJSON marshals the CompressionCodec a json representation.
func (cc CompressionCodec) MarshalJSON() ([]byte, error) {
	return json.Marshal(cc.String())
}

// String returns string representation of CompressionCodec
func (cc CompressionCodec) String() string {
	return []string{
		"none",
		"gzip",
		"snappy",
		"lz4",
		"zstd",
	}[int(cc)]
}

// Message is a kafka message type
type Message struct {
	Codec            CompressionCodec // codec used to compress the message contents
	CompressionLevel int              // compression level
	LogAppendTime    bool             // the used timestamp is LogAppendTime
	Key              []byte           // the message key, may be nil
	Value            []byte           // the message contents
	Set              *MessageSet      // the message set a message might wrap
	Version          int8             // v1 requires Kafka 0.10
	Timestamp        time.Time        // the timestamp of the message (version 1+ only)

	compressedSize int // used for computing the compression ratio metrics
}

// Decode decodes message from packet
func (m *Message) Decode(pd PacketDecoder) (err error) {
	crc32Decoder := acquireCrc32Field(crcIEEE)
	defer releaseCrc32Field(crc32Decoder)

	err = pd.push(crc32Decoder)
	if err != nil {
		return err
	}

	m.Version, err = pd.getInt8()
	if err != nil {
		return err
	}

	if m.Version > 1 {
		return PacketDecodingError{fmt.Sprintf("unknown magic byte (%v)", m.Version)}
	}

	attribute, err := pd.getInt8()
	if err != nil {
		return err
	}
	m.Codec = CompressionCodec(attribute & compressionCodecMask)
	m.LogAppendTime = attribute&timestampTypeMask == timestampTypeMask

	if m.Version == 1 {
		if err = (Timestamp{&m.Timestamp}).Decode(pd); err != nil {
			return err
		}
	}

	m.Key, err = pd.getBytes()
	if err != nil {
		return err
	}

	m.Value, err = pd.getBytes()
	if err != nil {
		return err
	}

	// Required for deep equal assertion during tests but might be useful
	// for future metrics about the compression ratio in fetch requests
	m.compressedSize = len(m.Value)

	switch m.Codec {
	case CompressionNone:
		// nothing to do
	default:
		if m.Value == nil {
			break
		}

		m.Value, err = decompress(m.Codec, m.Value)
		if err != nil {
			return err
		}
		if err := m.decodeSet(); err != nil {
			return err
		}
	}

	return pd.pop()
}

// decodes a message set from a previously encoded bulk-message
func (m *Message) decodeSet() (err error) {
	pd := RealDecoder{raw: m.Value}
	m.Set = &MessageSet{}
	return m.Set.Decode(&pd)
}
