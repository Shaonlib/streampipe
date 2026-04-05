package decoder

import (
	"fmt"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"
)

type Operation string

const (
	OpInsert Operation = "INSERT"
	OpUpdate Operation = "UPDATE"
	OpDelete Operation = "DELETE"
)

type ChangeEvent struct {
	LSN       pglogrepl.LSN
	Timestamp time.Time
	Schema    string
	Table     string
	Op        Operation
	Before    map[string]any
	After     map[string]any
}

func (e ChangeEvent) TableName() string {
	return e.Schema + "." + e.Table
}

type Decoder struct {
	relations map[uint32]*pglogrepl.RelationMessageV2
	typeMap   *pgtype.Map
}

func New() *Decoder {
	return &Decoder{
		relations: make(map[uint32]*pglogrepl.RelationMessageV2),
		typeMap:   pgtype.NewMap(),
	}
}

func (d *Decoder) Decode(lsn pglogrepl.LSN, msg pglogrepl.Message) (*ChangeEvent, error) {
	switch m := msg.(type) {
	case *pglogrepl.RelationMessageV2:
		d.relations[m.RelationID] = m
		return nil, nil
	case *pglogrepl.InsertMessageV2:
		rel, err := d.relation(m.RelationID)
		if err != nil {
			return nil, err
		}
		after, err := d.decodeRow(rel, m.Tuple)
		if err != nil {
			return nil, fmt.Errorf("INSERT decode: %w", err)
		}
		return &ChangeEvent{LSN: lsn, Schema: rel.Namespace, Table: rel.RelationName, Op: OpInsert, After: after}, nil
	case *pglogrepl.UpdateMessageV2:
		rel, err := d.relation(m.RelationID)
		if err != nil {
			return nil, err
		}
		var before map[string]any
		if m.OldTuple != nil {
			before, err = d.decodeRow(rel, m.OldTuple)
			if err != nil {
				return nil, fmt.Errorf("UPDATE old-row decode: %w", err)
			}
		}
		after, err := d.decodeRow(rel, m.NewTuple)
		if err != nil {
			return nil, fmt.Errorf("UPDATE new-row decode: %w", err)
		}
		return &ChangeEvent{LSN: lsn, Schema: rel.Namespace, Table: rel.RelationName, Op: OpUpdate, Before: before, After: after}, nil
	case *pglogrepl.DeleteMessageV2:
		rel, err := d.relation(m.RelationID)
		if err != nil {
			return nil, err
		}
		before, err := d.decodeRow(rel, m.OldTuple)
		if err != nil {
			return nil, fmt.Errorf("DELETE decode: %w", err)
		}
		return &ChangeEvent{LSN: lsn, Schema: rel.Namespace, Table: rel.RelationName, Op: OpDelete, Before: before}, nil
	default:
		return nil, nil
	}
}

func (d *Decoder) relation(id uint32) (*pglogrepl.RelationMessageV2, error) {
	rel, ok := d.relations[id]
	if !ok {
		return nil, fmt.Errorf("unknown relation ID %d", id)
	}
	return rel, nil
}

func (d *Decoder) decodeRow(rel *pglogrepl.RelationMessageV2, tuple *pglogrepl.TupleData) (map[string]any, error) {
	if tuple == nil {
		return nil, nil
	}
	row := make(map[string]any, len(rel.Columns))
	for i, col := range tuple.Columns {
		if i >= len(rel.Columns) {
			break
		}
		name := rel.Columns[i].Name
		switch col.DataType {
		case 'n':
			row[name] = nil
		case 'u':
			row[name] = "<unchanged-toast>"
		case 't':
			oid := rel.Columns[i].DataType
			dt, ok := d.typeMap.TypeForOID(oid)
			if !ok {
				row[name] = string(col.Data)
				continue
			}
			val, err := dt.Codec.DecodeDatabaseSQLValue(d.typeMap, oid, pgtype.TextFormatCode, col.Data)
			if err != nil {
				row[name] = string(col.Data)
				continue
			}
			row[name] = val
		}
	}
	return row, nil
}
