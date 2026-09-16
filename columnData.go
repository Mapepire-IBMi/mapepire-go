package mapepire

import "encoding/json"

type ColumnData struct {
	any
}

// UnmarshalJSON implements encoding/json.Unmarshaler so decoded column values
// are stored in the embedded any field instead of being discarded.
func (c *ColumnData) UnmarshalJSON(data []byte) error {
	return json.Unmarshal(data, &c.any)
}

// Converts the ColumnData to BlobRef
// delivers empty BlobRef if not a BlobRef
func (c ColumnData) ToBlobRef() BlobRef {
	data, ok := c.any.(map[string]any)
	if !ok {
		return BlobRef{}
	}

	blobURL, ok := data["blob_url"].(string)
	if !ok {
		return BlobRef{}
	}

	size, ok := data["size"].(float64)
	if !ok || size < 0 || size != float64(int(size)) {
		return BlobRef{}
	}

	return BlobRef{BlobUrl: blobURL, Size: int(size)}
}
