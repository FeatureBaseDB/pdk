package csrc

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/pkg/errors"
)

type Client struct {
	URL string

	httpClient *http.Client
}

func NewClient(url string, tlsConfig *tls.Config) *Client {
	if !strings.HasPrefix(url, "http") {
		url = "http://" + url
	}
	c := http.DefaultClient
	if strings.HasPrefix(url, "https://") {
		fmt.Println("getting http client with tls config", tlsConfig)
		c = getHTTPClient(tlsConfig)
	}
	return &Client{
		URL: url,

		httpClient: c,
	}
}

// GetSchema gets the schema with the ID.
// https://docs.confluent.io/current/schema-registry/develop/api.html#get--schemas-ids-int-%20id
func (c *Client) GetSchema(id int) (string, error) {
	sr := SchemaResponse{}
	resp, err := c.httpClient.Get(fmt.Sprintf("%s/schemas/ids/%d", c.URL, id))
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
	resp, err := c.httpClient.Post(fmt.Sprintf("%s/subjects/%s/versions", c.URL, subj), "application/json", strings.NewReader(schema))
	sr := &SchemaResponse{}
	err = unmarshalRespErr(resp, err, sr)
	if err != nil {
		return nil, errors.Wrapf(err, "unmarshaling resp to %s", fmt.Sprintf("%s/subjects/%s/versions", c.URL, subj))
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

func getHTTPClient(t *tls.Config) *http.Client {
	transport := &http.Transport{
		Dial: (&net.Dialer{
			Timeout: time.Second * 20,
		}).Dial,
	}
	if t != nil {
		transport.TLSClientConfig = t
	}
	return &http.Client{Transport: transport}
}
