package kafka

import "fmt"

const (
	unknownRecords = iota
	legacyRecords
	defaultRecords

	magicOffset = 16
)

// Records implements a union type containing either a RecordBatch or a legacy MessageSet.
type Records struct {
	RecordsType int
	MsgSet      *MessageSet
	RecordBatch *RecordBatch
}

func (r *Records) setTypeFromMagic(pd PacketDecoder) error {
	magic, err := magicValue(pd)
	if err != nil {
		return err
	}

	r.RecordsType = defaultRecords
	if magic < 2 {
		r.RecordsType = legacyRecords
	}

	return nil
}

func magicValue(pd PacketDecoder) (int8, error) {
	return pd.peekInt8(magicOffset)
}

func (r *Records) decode(pd PacketDecoder) error {
	if r.RecordsType == unknownRecords {
		if err := r.setTypeFromMagic(pd); err != nil {
			return err
		}
	}

	switch r.RecordsType {
	case legacyRecords:
		r.MsgSet = &MessageSet{}
		return r.MsgSet.Decode(pd)
	case defaultRecords:
		r.RecordBatch = &RecordBatch{}
		return r.RecordBatch.decode(pd)
	}
	return fmt.Errorf("unknown Records type: %v", r.RecordsType)
}
