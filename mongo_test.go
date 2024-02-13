package mongo

import (
	"testing"

	"github.com/kapetacom/sdk-go-config/providers"
	"github.com/stretchr/testify/assert"
)

func TestGetProtocol(t *testing.T) {
	t.Run("default protocol", func(t *testing.T) {
		resInfo := &providers.ResourceInfo{}
		expected := "mongodb"
		actual := getProtocol(resInfo)
		assert.Equal(t, expected, actual)
	})
	t.Run("protocol specified in options", func(t *testing.T) {
		resInfo := &providers.ResourceInfo{Options: map[string]interface{}{"protocol": "mongodb+srv"}}
		expected := "mongodb+srv"
		actual := getProtocol(resInfo)
		assert.Equal(t, expected, actual)
	})
}
