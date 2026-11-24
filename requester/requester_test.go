// Copyright 2014 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package requester

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestN(t *testing.T) {
	var count int64
	handler := func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&count, int64(1))
	}
	server := httptest.NewServer(http.HandlerFunc(handler))
	defer server.Close()

	req, _ := http.NewRequest("GET", server.URL, nil)
	w := &Work{
		Request: req,
		N:       20,
		C:       2,
	}
	w.Run()
	if count != 20 {
		t.Errorf("Expected to send 20 requests, found %v", count)
	}
}

func TestQps(t *testing.T) {
	var wg sync.WaitGroup
	var count int64
	handler := func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&count, int64(1))
	}
	server := httptest.NewServer(http.HandlerFunc(handler))
	defer server.Close()

	req, _ := http.NewRequest("GET", server.URL, nil)
	w := &Work{
		Request: req,
		N:       20,
		C:       2,
		QPS:     1,
	}
	wg.Add(1)
	time.AfterFunc(time.Second, func() {
		if count > 2 {
			t.Errorf("Expected to work at most 2 times, found %v", count)
		}
		wg.Done()
	})
	go w.Run()
	wg.Wait()
}

func TestRequest(t *testing.T) {
	var uri, contentType, some, auth string
	handler := func(w http.ResponseWriter, r *http.Request) {
		uri = r.RequestURI
		contentType = r.Header.Get("Content-type")
		some = r.Header.Get("X-some")
		auth = r.Header.Get("Authorization")
	}
	server := httptest.NewServer(http.HandlerFunc(handler))
	defer server.Close()

	header := make(http.Header)
	header.Add("Content-type", "text/html")
	header.Add("X-some", "value")
	req, _ := http.NewRequest("GET", server.URL, nil)
	req.Header = header
	req.SetBasicAuth("username", "password")
	w := &Work{
		Request: req,
		N:       1,
		C:       1,
	}
	w.Run()
	if uri != "/" {
		t.Errorf("Uri is expected to be /, %v is found", uri)
	}
	if contentType != "text/html" {
		t.Errorf("Content type is expected to be text/html, %v is found", contentType)
	}
	if some != "value" {
		t.Errorf("X-some header is expected to be value, %v is found", some)
	}
	if auth != "Basic dXNlcm5hbWU6cGFzc3dvcmQ=" {
		t.Errorf("Basic authorization is not properly set")
	}
}

func TestBody(t *testing.T) {
	var count int64
	handler := func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if string(body) == "Body" {
			atomic.AddInt64(&count, 1)
		}
	}
	server := httptest.NewServer(http.HandlerFunc(handler))
	defer server.Close()

	req, _ := http.NewRequest("POST", server.URL, bytes.NewBuffer([]byte("Body")))
	w := &Work{
		Request:     req,
		RequestBody: []byte("Body"),
		N:           10,
		C:           1,
	}
	w.Run()
	if count != 10 {
		t.Errorf("Expected to work 10 times, found %v", count)
	}
}

// Status counts and averages include successful results beyond the sample cap.
func TestReportBeyondSampleLimit(t *testing.T) {
	var output bytes.Buffer
	results := make(chan *result)
	r := newReport(&output, results, `{
  "NumRes": {{.NumRes}}, "Rps": {{.Rps}},
  "ErrorDist": {{jsonify .ErrorDist}}, "StatusCodeDist": {{jsonify .StatusCodeDist}},
  "Average": {{.Average}}, "AvgConn": {{.AvgConn}}, "AvgDNS": {{.AvgDNS}},
  "AvgReq": {{.AvgReq}}, "AvgDelay": {{.AvgDelay}}, "AvgRes": {{.AvgRes}},
  "SizeReq": {{.SizeReq}}, "SizeTotal": {{.SizeTotal}}
}`, maxRes+3)
	go runReporter(r)
	first := &result{
		statusCode: http.StatusOK,
		duration: time.Second,
		connDuration: 2 * time.Second,
		dnsDuration: 3 * time.Second,
		reqDuration: 4 * time.Second,
		delayDuration: 5 * time.Second,
		resDuration: 6 * time.Second,
		contentLength: maxRes,
	}
	for i := 0; i < maxRes; i++ {
		results <- first
	}
	last := *first
	last.statusCode = http.StatusInternalServerError
	last.duration *= 2
	last.connDuration *= 2
	last.dnsDuration *= 2
	last.reqDuration *= 2
	last.delayDuration *= 2
	last.resDuration *= 2
	results <- &last
	later := *first
	later.duration *= 3
	later.connDuration *= 3
	later.dnsDuration *= 3
	later.reqDuration *= 3
	later.delayDuration *= 3
	later.resDuration *= 3
	results <- &later
	results <- &result{err: errors.New("request failed")}
	close(results)
	<-r.done
	r.finalize(time.Second)

	var got Report
	if err := json.Unmarshal(output.Bytes(), &got); err != nil {
		t.Fatalf("decode report: %v", err)
	}
	if got.NumRes != maxRes+3 || got.Rps != float64(maxRes+3) {
		t.Errorf("request count/rate = %d/%g, want %d", got.NumRes, got.Rps, maxRes+3)
	}
	if len(got.ErrorDist) != 1 || got.ErrorDist["request failed"] != 1 {
		t.Errorf("error distribution = %v, want one failed request", got.ErrorDist)
	}
	if len(got.StatusCodeDist) != 2 || got.StatusCodeDist[http.StatusOK] != maxRes+1 || got.StatusCodeDist[http.StatusInternalServerError] != 1 {
		t.Errorf("status distribution = %v, want %d OK and one server error", got.StatusCodeDist, maxRes+1)
	}
	for name, average := range map[string]float64{
		"total": got.Average,
		"connection": got.AvgConn / 2,
		"DNS": got.AvgDNS / 3,
		"request": got.AvgReq / 4,
		"delay": got.AvgDelay / 5,
		"response": got.AvgRes / 6,
	} {
		want := float64(maxRes+5) / float64(maxRes+2)
		if math.Abs(average-want) > 1e-12 {
			t.Errorf("normalized %s average = %g, want %g", name, average, want)
		}
	}
	if got.SizeReq != maxRes || got.SizeTotal != int64(maxRes)*(maxRes+2) {
		t.Errorf("response sizes = %d/%d, want %d/%d", got.SizeReq, got.SizeTotal, maxRes, int64(maxRes)*(maxRes+2))
	}
	if len(r.lats) != maxRes || len(r.statusCodes) != maxRes {
		t.Errorf("sample counts = %d/%d, want %d", len(r.lats), len(r.statusCodes), maxRes)
	}
}
