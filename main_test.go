package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func testServer() http.Handler {
	return NewAPI(NewMemoryUserStore(), NewMemoryReadingStore()).routes()
}

func TestCreateAndGetUser(t *testing.T) {
	server := testServer()
	body := bytes.NewBufferString(`{"username":"air_admin","password_hash":"hashed-password","role":"admin","alert_threshold":75}`)

	createRequest := httptest.NewRequest(http.MethodPost, "/users", body)
	createResponse := httptest.NewRecorder()
	server.ServeHTTP(createResponse, createRequest)

	if createResponse.Code != http.StatusCreated {
		t.Fatalf("expected status 201, got %d: %s", createResponse.Code, createResponse.Body.String())
	}

	getRequest := httptest.NewRequest(http.MethodGet, "/users/air_admin", nil)
	getResponse := httptest.NewRecorder()
	server.ServeHTTP(getResponse, getRequest)

	if getResponse.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", getResponse.Code, getResponse.Body.String())
	}

	var user User
	if err := json.NewDecoder(getResponse.Body).Decode(&user); err != nil {
		t.Fatal(err)
	}
	if user.Username != "air_admin" || user.PasswordHash != "hashed-password" || user.Role != "admin" || user.AlertThreshold != 75 {
		t.Fatalf("unexpected user payload: %+v", user)
	}
}

func TestCreateUserRejectsDuplicates(t *testing.T) {
	server := testServer()
	payload := `{"username":"air_admin","password_hash":"hashed-password"}`

	for i, expected := range []int{http.StatusCreated, http.StatusConflict} {
		request := httptest.NewRequest(http.MethodPost, "/users", bytes.NewBufferString(payload))
		response := httptest.NewRecorder()
		server.ServeHTTP(response, request)
		if response.Code != expected {
			t.Fatalf("request %d expected status %d, got %d", i+1, expected, response.Code)
		}
	}
}

func TestCreateAndListReadings(t *testing.T) {
	server := testServer()
	body := bytes.NewBufferString(`{"pm25":12.5,"co2":700,"aqi":13.25,"category":"good","location":"campus","source":"test","user_id":"1","alert_threshold":20,"alert_triggered":false}`)

	createRequest := httptest.NewRequest(http.MethodPost, "/readings", body)
	createResponse := httptest.NewRecorder()
	server.ServeHTTP(createResponse, createRequest)

	if createResponse.Code != http.StatusCreated {
		t.Fatalf("expected status 201, got %d: %s", createResponse.Code, createResponse.Body.String())
	}

	otherBody := bytes.NewBufferString(`{"pm25":40,"co2":900,"aqi":29,"category":"good","location":"lab","source":"test","user_id":"2"}`)
	otherRequest := httptest.NewRequest(http.MethodPost, "/readings", otherBody)
	otherResponse := httptest.NewRecorder()
	server.ServeHTTP(otherResponse, otherRequest)
	if otherResponse.Code != http.StatusCreated {
		t.Fatalf("expected second reading status 201, got %d", otherResponse.Code)
	}

	listRequest := httptest.NewRequest(http.MethodGet, "/readings?user_id=1&limit=10", nil)
	listResponse := httptest.NewRecorder()
	server.ServeHTTP(listResponse, listRequest)

	if listResponse.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", listResponse.Code, listResponse.Body.String())
	}
	if !bytes.Contains(listResponse.Body.Bytes(), []byte(`"location":"campus"`)) {
		t.Fatalf("expected stored reading in response: %s", listResponse.Body.String())
	}
	if bytes.Contains(listResponse.Body.Bytes(), []byte(`"location":"lab"`)) {
		t.Fatalf("expected user_id filter to exclude other user reading: %s", listResponse.Body.String())
	}
}

func TestCreateReadingRejectsInvalidValues(t *testing.T) {
	server := testServer()
	body := bytes.NewBufferString(`{"pm25":-1,"co2":700,"aqi":13.25}`)

	request := httptest.NewRequest(http.MethodPost, "/readings", body)
	response := httptest.NewRecorder()
	server.ServeHTTP(response, request)

	if response.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d", response.Code)
	}
}

func TestBuildStoresFailsWhenConfiguredPostgresIsUnavailable(t *testing.T) {
	t.Setenv("POSTGRES_URL", "postgres://user:password@127.0.0.1:1/airpure_users?sslmode=disable")
	t.Setenv("DB_CONNECT_RETRIES", "1")
	t.Setenv("DB_RETRY_DELAY_MS", "1")
	t.Setenv("INFLUX_URL", "")
	t.Setenv("INFLUX_ORG", "")
	t.Setenv("INFLUX_BUCKET", "")
	t.Setenv("INFLUX_TOKEN", "")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, _, err := buildStores(ctx)
	if err == nil {
		t.Fatal("expected configured PostgreSQL startup to fail")
	}
	if !bytes.Contains([]byte(err.Error()), []byte("postgres is required")) {
		t.Fatalf("expected required postgres error, got %v", err)
	}
}

func TestBuildStoresFailsWhenConfiguredInfluxIsUnavailable(t *testing.T) {
	t.Setenv("POSTGRES_URL", "")
	t.Setenv("INFLUX_URL", "http://127.0.0.1:1")
	t.Setenv("INFLUX_ORG", "airpure")
	t.Setenv("INFLUX_BUCKET", "sensor_data")
	t.Setenv("INFLUX_TOKEN", "token")
	t.Setenv("INFLUX_CONNECT_RETRIES", "1")
	t.Setenv("INFLUX_RETRY_DELAY_MS", "1")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, _, err := buildStores(ctx)
	if err == nil {
		t.Fatal("expected configured InfluxDB startup to fail")
	}
	if !bytes.Contains([]byte(err.Error()), []byte("influxdb is required")) {
		t.Fatalf("expected required influx error, got %v", err)
	}
}
