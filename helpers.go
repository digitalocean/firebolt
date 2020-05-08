package firebolt

import (
	"fmt"
	"strconv"
)

// Nodeconfig holds a Nodes configuration
type Nodeconfig map[string]string

// IntConfig validates and fetches the int-typed optional config value specified by 'name', using the 'defaultValue' if
// no value was provided in the configuration.
func (c Nodeconfig) IntConfig(name string, defaultValue int, minValue int, maxValue int) (int, error) {
	// set the default value, if not provided
	_, ok := c[name]
	if !ok {
		c[name] = strconv.Itoa(defaultValue)
	}

	return c.IntConfigRequired(name, minValue, maxValue)
}

// IntConfigRequired validates and fetches the int-typed required config value specified by 'name', returning an error
// if no value was provided in the configuration.
func (c Nodeconfig) IntConfigRequired(name string, minValue int, maxValue int) (int, error) {
	userValue, ok := c[name]
	if !ok {
		return 0, fmt.Errorf("missing config value [%s]", name)
	}

	intValue, err := strconv.Atoi(userValue)
	if err != nil {
		return 0, fmt.Errorf("expected integer value for config [%s]", name)
	}

	if intValue > maxValue || intValue < minValue {
		return 0, fmt.Errorf("config value [%s] requires value between [%d] and [%d]", name, minValue, maxValue)
	}
	return intValue, nil
}

// StringConfig validates and fetches the string-typed optional config value specified by 'name', using the 'defaultValue' if
// no value was provided in the configuration.
func (c Nodeconfig) StringConfig(name string, defaultValue string) (string, error) {
	// set the default value, if not provided
	_, ok := c[name]
	if !ok {
		c[name] = defaultValue
	}

	return c.StringConfigRequired(name)
}

// StringConfigRequired validates and fetches the string-typed required config value specified by 'name', returning an
// error if no value was provided in the configuration.
func (c Nodeconfig) StringConfigRequired(name string) (string, error) {
	userValue, ok := c[name]
	if !ok {
		return "", fmt.Errorf("missing config value [%s]", name)
	}
	return userValue, nil
}
