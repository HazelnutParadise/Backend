package jsonutil

import (
	"encoding/json"
	"errors"
	"os"
)

// LoadJSON 讀取 JSON 文件並將其解析為 map[string]interface{}
func LoadJSON(filePath string) (map[string]interface{}, error) {
	file, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	var data map[string]interface{}
	if err := json.Unmarshal(file, &data); err != nil {
		return nil, err
	}

	return data, nil
}

// LoadAndQueryJSON 讀取 JSON 文件，並根據鍵路徑返回對應的子 map
func LoadAndQueryJSON(filePath string, keys ...string) (map[string]interface{}, error) {
	// 先調用 LoadJSON 讀取 JSON 文件
	data, err := LoadJSON(filePath)
	if err != nil {
		return nil, err
	}

	// 根據鍵路徑檢索子 map
	currentMap := data
	for _, key := range keys {
		if val, exists := currentMap[key]; exists {
			switch v := val.(type) {
			case map[string]interface{}:
				currentMap = v
			default:
				return nil, errors.New("key does not point to a map")
			}
		} else {
			return nil, errors.New("key not found")
		}
	}
	return currentMap, nil
}
