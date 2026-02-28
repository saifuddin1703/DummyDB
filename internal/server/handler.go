package server

import (
	"fmt"

	"github.com/dummydb/internal/database"
)

// Handler handles command execution
type Handler struct {
	db database.Database
}

// NewHandler creates a new command handler
func NewHandler(db database.Database) *Handler {
	return &Handler{
		db: db,
	}
}

// Execute executes a parsed command
func (h *Handler) Execute(cmd *Command) ([]byte, error) {
	switch cmd.Type {
	case CommandSet:
		return h.handleSet(cmd)

	case CommandGet:
		return h.handleGet(cmd)

	case CommandDelete:
		return h.handleDelete(cmd)

	case CommandKeys:
		return h.handleKeys(cmd)

	default:
		return nil, fmt.Errorf("unknown command type")
	}
}

func (h *Handler) handleSet(cmd *Command) ([]byte, error) {
	err := h.db.Put(cmd.Key, []byte(cmd.Value))
	if err != nil {
		return nil, fmt.Errorf("error setting key %s: %w", cmd.Key, err)
	}

	return []byte(cmd.Value), nil
}

func (h *Handler) handleGet(cmd *Command) ([]byte, error) {
	val, err := h.db.Get(cmd.Key)
	if err != nil {
		return nil, fmt.Errorf("error getting key %s: %w", cmd.Key, err)
	}

	return val, nil
}

func (h *Handler) handleDelete(cmd *Command) ([]byte, error) {
	err := h.db.Delete(cmd.Key)
	if err != nil {
		return nil, fmt.Errorf("error deleting key %s: %w", cmd.Key, err)
	}

	return []byte("key deleted"), nil
}

func (h *Handler) handleKeys(_ *Command) ([]byte, error) {
	keys, err := h.db.Keys()
	if err != nil {
		return nil, fmt.Errorf("error getting keys: %w", err)
	}

	return keys, nil
}
