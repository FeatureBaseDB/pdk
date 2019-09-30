package csrc

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/pkg/errors"
)

type Client struct {
	URL string
}

func NewClient(url string) *Client {
	if !strings.HasPrefix(url, "http") {
		url = "http://" + url
	}
	return &Client{
		URL: url,
	}
}

// GetSchema gets the schema with the ID.
// https://docs.confluent.io/current/schema-registry/develop/api.html#get--schemas-ids-int-%20id
func (c *Client) GetSchema(id int) (string, error) {
	sr := SchemaResponse{}
	resp, err := http.Get(fmt.Sprintf("%s/schemas/ids/%d", c.URL, id))
	err = unmarshalRespErr(resp, err, &sr)
	if err != nil {
		return "", errors.Wrap(err, "making http request")
	}
	return sr.Schema, nil
}

type SchemaResponse struct {
	Schema  string `json:"schema"`  // The actual AVRO schema
	Subject string `json:"subject"` // Subject where the schema is registered for
	Version int    `json:"version"` // Version within this subject
	ID      int    `json:"id"`      // Registry's unique id
}

type ErrorResponse struct {
	StatusCode int    `json:"error_code"`
	Body       string `json:"message"`
}

func (e *ErrorResponse) Error() string {
	return fmt.Sprintf("status %d: %s", e.StatusCode, e.Body)
}

func (c *Client) PostSubjects(subj, schema string) (*SchemaResponse, error) {
	schema = strings.Replace(schema, "\t", "", -1)
	schema = strings.Replace(schema, "\n", `\n`, -1)
	schema = fmt.Sprintf(`{"schema": "%s"}`, strings.Replace(schema, `"`, `\"`, -1)) // this is probably terrible
	resp, err := http.Post(fmt.Sprintf("%s/subjects/%s/versions", c.URL, subj), "application/json", strings.NewReader(schema))
	sr := &SchemaResponse{}
	err = unmarshalRespErr(resp, err, sr)
	if err != nil {
		return nil, errors.Wrap(err, "unmarshaling resp")
	}
	return sr, nil
}

func unmarshalRespErr(resp *http.Response, err error, into interface{}) error {
	if err != nil {
		return errors.Wrap(err, "making http request")
	}
	if resp.StatusCode != 200 {
		bod, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return errors.Wrap(err, "reading body")
		}
		errResp := &ErrorResponse{
			StatusCode: resp.StatusCode,
			Body:       string(bod),
		}
		return errResp
	}
	dec := json.NewDecoder(resp.Body)
	err = dec.Decode(into)
	if err != nil {
		return errors.Wrap(err, "unmarshaling body")
	}
	return nil
}
