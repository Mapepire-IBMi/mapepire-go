package mapepire

import "encoding/base64"

type blob string

func NewBlob(bytes []byte) blob {
	return blob(base64.StdEncoding.EncodeToString(bytes))
}
