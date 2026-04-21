package server

import (
	"fmt"
	"strings"
)

// Command represents a parsed command
type Command struct {
	Type  CommandType
	Key   string
	Value string
}

// CommandType represents the type of command
type CommandType int

const (
	CommandUnknown CommandType = iota
	CommandSet
	CommandGet
	CommandDelete
	CommandKeys
)

// ParseCommand parses a command string into a Command
func ParseCommand(data string) (*Command, error) {
	data = strings.TrimSpace(data)
	if data == "" {
		return nil, fmt.Errorf("empty command")
	}

	parts := strings.Split(data, " ")
	if len(parts) < 1 || len(parts) > 3 {
		return nil, fmt.Errorf("invalid command format")
	}

	cmd := &Command{}
	cmdName := strings.ToLower(parts[0])

	switch cmdName {
	case "set":
		if len(parts) != 3 {
			return nil, fmt.Errorf("SET requires key and value")
		}
		cmd.Type = CommandSet
		cmd.Key = parts[1]
		cmd.Value = parts[2]

	case "get":
		if len(parts) != 2 {
			return nil, fmt.Errorf("GET requires key")
		}
		cmd.Type = CommandGet
		cmd.Key = strings.TrimSuffix(parts[1], "\r\n")

	case "del", "delete":
		if len(parts) != 2 {
			return nil, fmt.Errorf("DELETE requires key")
		}
		cmd.Type = CommandDelete
		cmd.Key = strings.TrimSuffix(parts[1], "\r\n")

	case "keys":
		if len(parts) != 1 {
			return nil, fmt.Errorf("KEYS takes no arguments")
		}
		cmd.Type = CommandKeys

	default:
		return nil, fmt.Errorf("unknown command: %s", cmdName)
	}

	return cmd, nil
}
