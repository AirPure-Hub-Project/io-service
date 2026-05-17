package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

type User struct {
	ID             int64     `json:"id"`
	Username       string    `json:"username"`
	PasswordHash   string    `json:"password_hash"`
	Role           string    `json:"role"`
	AlertThreshold float64   `json:"alert_threshold"`
	CreatedAt      time.Time `json:"created_at"`
}

type Reading struct {
	ID             int64     `json:"id"`
	PM25           float64   `json:"pm25"`
	CO2            float64   `json:"co2"`
	AQI            float64   `json:"aqi"`
	Category       string    `json:"category"`
	Location       string    `json:"location"`
	Source         string    `json:"source"`
	UserID         string    `json:"user_id,omitempty"`
	AlertThreshold float64   `json:"alert_threshold"`
	AlertTriggered bool      `json:"alert_triggered"`
	AlertMessage   string    `json:"alert_message,omitempty"`
	ProcessedAt    time.Time `json:"processed_at"`
	CreatedAt      time.Time `json:"created_at"`
}

type ReadingFilter struct {
	UserID string
	Limit  int
}

type UserStore interface {
	CreateUser(ctx context.Context, user User) (User, error)
	FindUser(ctx context.Context, username string) (User, error)
	Mode() string
}

type ReadingStore interface {
	CreateReading(ctx context.Context, reading Reading) (Reading, error)
	ListReadings(ctx context.Context, filter ReadingFilter) ([]Reading, error)
	Mode() string
}

var ErrNotFound = errors.New("not found")
var ErrConflict = errors.New("conflict")

type MemoryUserStore struct {
	mu     sync.Mutex
	users  map[string]User
	nextID int64
}

func NewMemoryUserStore() *MemoryUserStore {
	return &MemoryUserStore{users: map[string]User{}, nextID: 1}
}

func (s *MemoryUserStore) CreateUser(ctx context.Context, user User) (User, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.users[user.Username]; exists {
		return User{}, ErrConflict
	}

	user.ID = s.nextID
	user.Role = defaultString(user.Role, "user")
	user.AlertThreshold = defaultFloat(user.AlertThreshold, 100)
	user.CreatedAt = time.Now().UTC()
	s.nextID++
	s.users[user.Username] = user
	return user, nil
}

func (s *MemoryUserStore) FindUser(ctx context.Context, username string) (User, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	user, exists := s.users[username]
	if !exists {
		return User{}, ErrNotFound
	}
	return user, nil
}

func (s *MemoryUserStore) Mode() string {
	return "memory"
}

type PostgresStore struct {
	db *sql.DB
}

func NewPostgresStore(ctx context.Context, databaseURL string) (*PostgresStore, error) {
	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return nil, err
	}

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, err
	}

	store := &PostgresStore{db: db}
	if err := store.init(ctx); err != nil {
		db.Close()
		return nil, err
	}
	return store, nil
}

func (s *PostgresStore) init(ctx context.Context) error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS users (
			id SERIAL PRIMARY KEY,
			username TEXT NOT NULL UNIQUE,
			password_hash TEXT NOT NULL,
			role TEXT NOT NULL DEFAULT 'user',
			alert_threshold DOUBLE PRECISION NOT NULL DEFAULT 100,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,
		`ALTER TABLE users ADD COLUMN IF NOT EXISTS role TEXT NOT NULL DEFAULT 'user'`,
		`ALTER TABLE users ADD COLUMN IF NOT EXISTS alert_threshold DOUBLE PRECISION NOT NULL DEFAULT 100`,
		`CREATE TABLE IF NOT EXISTS readings (
			id SERIAL PRIMARY KEY,
			pm25 DOUBLE PRECISION NOT NULL,
			co2 DOUBLE PRECISION NOT NULL,
			aqi DOUBLE PRECISION NOT NULL,
			category TEXT NOT NULL,
			location TEXT NOT NULL,
			source TEXT NOT NULL,
			user_id TEXT,
			alert_threshold DOUBLE PRECISION NOT NULL DEFAULT 100,
			alert_triggered BOOLEAN NOT NULL DEFAULT FALSE,
			alert_message TEXT,
			processed_at TIMESTAMPTZ NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,
		`ALTER TABLE readings ADD COLUMN IF NOT EXISTS alert_threshold DOUBLE PRECISION NOT NULL DEFAULT 100`,
		`ALTER TABLE readings ADD COLUMN IF NOT EXISTS alert_triggered BOOLEAN NOT NULL DEFAULT FALSE`,
		`ALTER TABLE readings ADD COLUMN IF NOT EXISTS alert_message TEXT`,
	}

	for _, statement := range statements {
		if _, err := s.db.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}

func (s *PostgresStore) CreateUser(ctx context.Context, user User) (User, error) {
	user.Role = defaultString(user.Role, "user")
	user.AlertThreshold = defaultFloat(user.AlertThreshold, 100)
	err := s.db.QueryRowContext(
		ctx,
		`INSERT INTO users (username, password_hash, role, alert_threshold)
		 VALUES ($1, $2, $3, $4)
		 RETURNING id, username, password_hash, role, alert_threshold, created_at`,
		user.Username,
		user.PasswordHash,
		user.Role,
		user.AlertThreshold,
	).Scan(&user.ID, &user.Username, &user.PasswordHash, &user.Role, &user.AlertThreshold, &user.CreatedAt)
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "unique") {
			return User{}, ErrConflict
		}
		return User{}, err
	}
	return user, nil
}

func (s *PostgresStore) FindUser(ctx context.Context, username string) (User, error) {
	var user User
	err := s.db.QueryRowContext(
		ctx,
		`SELECT id, username, password_hash, role, alert_threshold, created_at FROM users WHERE username = $1`,
		username,
	).Scan(&user.ID, &user.Username, &user.PasswordHash, &user.Role, &user.AlertThreshold, &user.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return User{}, ErrNotFound
	}
	return user, err
}

func (s *PostgresStore) CreateReading(ctx context.Context, reading Reading) (Reading, error) {
	reading.Category = defaultString(reading.Category, "unknown")
	reading.Location = defaultString(reading.Location, "unknown")
	reading.Source = defaultString(reading.Source, "api")
	reading.AlertThreshold = defaultFloat(reading.AlertThreshold, 100)
	if reading.ProcessedAt.IsZero() {
		reading.ProcessedAt = time.Now().UTC()
	}

	err := s.db.QueryRowContext(
		ctx,
		`INSERT INTO readings (
			pm25, co2, aqi, category, location, source, user_id,
			alert_threshold, alert_triggered, alert_message, processed_at
		) VALUES ($1, $2, $3, $4, $5, $6, NULLIF($7, ''), $8, $9, NULLIF($10, ''), $11)
		 RETURNING id, pm25, co2, aqi, category, location, source, COALESCE(user_id, ''),
		 alert_threshold, alert_triggered, COALESCE(alert_message, ''), processed_at, created_at`,
		reading.PM25,
		reading.CO2,
		reading.AQI,
		reading.Category,
		reading.Location,
		reading.Source,
		reading.UserID,
		reading.AlertThreshold,
		reading.AlertTriggered,
		reading.AlertMessage,
		reading.ProcessedAt,
	).Scan(
		&reading.ID,
		&reading.PM25,
		&reading.CO2,
		&reading.AQI,
		&reading.Category,
		&reading.Location,
		&reading.Source,
		&reading.UserID,
		&reading.AlertThreshold,
		&reading.AlertTriggered,
		&reading.AlertMessage,
		&reading.ProcessedAt,
		&reading.CreatedAt,
	)
	return reading, err
}

func (s *PostgresStore) ListReadings(ctx context.Context, filter ReadingFilter) ([]Reading, error) {
	filter.Limit = normalizeLimit(filter.Limit)

	var rows *sql.Rows
	var err error
	if filter.UserID != "" {
		rows, err = s.db.QueryContext(
			ctx,
			`SELECT id, pm25, co2, aqi, category, location, source, COALESCE(user_id, ''),
			 alert_threshold, alert_triggered, COALESCE(alert_message, ''), processed_at, created_at
			 FROM readings WHERE user_id = $1 ORDER BY created_at DESC LIMIT $2`,
			filter.UserID,
			filter.Limit,
		)
	} else {
		rows, err = s.db.QueryContext(
			ctx,
			`SELECT id, pm25, co2, aqi, category, location, source, COALESCE(user_id, ''),
			 alert_threshold, alert_triggered, COALESCE(alert_message, ''), processed_at, created_at
			 FROM readings ORDER BY created_at DESC LIMIT $1`,
			filter.Limit,
		)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	readings := []Reading{}
	for rows.Next() {
		var reading Reading
		if err := rows.Scan(
			&reading.ID,
			&reading.PM25,
			&reading.CO2,
			&reading.AQI,
			&reading.Category,
			&reading.Location,
			&reading.Source,
			&reading.UserID,
			&reading.AlertThreshold,
			&reading.AlertTriggered,
			&reading.AlertMessage,
			&reading.ProcessedAt,
			&reading.CreatedAt,
		); err != nil {
			return nil, err
		}
		readings = append(readings, reading)
	}
	return readings, rows.Err()
}

func (s *PostgresStore) Mode() string {
	return "postgres"
}

type MemoryReadingStore struct {
	mu       sync.Mutex
	readings []Reading
	nextID   int64
}

func NewMemoryReadingStore() *MemoryReadingStore {
	return &MemoryReadingStore{readings: []Reading{}, nextID: 1}
}

func (s *MemoryReadingStore) CreateReading(ctx context.Context, reading Reading) (Reading, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	reading.ID = s.nextID
	reading.Category = defaultString(reading.Category, "unknown")
	reading.Location = defaultString(reading.Location, "unknown")
	reading.Source = defaultString(reading.Source, "api")
	reading.AlertThreshold = defaultFloat(reading.AlertThreshold, 100)
	if reading.ProcessedAt.IsZero() {
		reading.ProcessedAt = time.Now().UTC()
	}
	reading.CreatedAt = time.Now().UTC()
	s.nextID++
	s.readings = append(s.readings, reading)
	return reading, nil
}

func (s *MemoryReadingStore) ListReadings(ctx context.Context, filter ReadingFilter) ([]Reading, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	limit := normalizeLimit(filter.Limit)
	readings := make([]Reading, 0, len(s.readings))
	for i := len(s.readings) - 1; i >= 0 && len(readings) < limit; i-- {
		reading := s.readings[i]
		if filter.UserID != "" && reading.UserID != filter.UserID {
			continue
		}
		readings = append(readings, reading)
	}
	return readings, nil
}

func (s *MemoryReadingStore) Mode() string {
	return "memory"
}

type InfluxWriter struct {
	url    string
	org    string
	bucket string
	token  string
	client *http.Client
}

func NewInfluxWriter(baseURL, org, bucket, token string) *InfluxWriter {
	return &InfluxWriter{
		url:    strings.TrimRight(baseURL, "/"),
		org:    org,
		bucket: bucket,
		token:  token,
		client: &http.Client{Timeout: 3 * time.Second},
	}
}

func (s *InfluxWriter) WriteReading(ctx context.Context, reading Reading) error {
	line := fmt.Sprintf(
		"air_quality,location=%s,source=%s pm25=%f,co2=%f,aqi=%f,alert_threshold=%f,alert_triggered=%t,category=\"%s\" %d",
		escapeInfluxTag(reading.Location),
		escapeInfluxTag(reading.Source),
		reading.PM25,
		reading.CO2,
		reading.AQI,
		reading.AlertThreshold,
		reading.AlertTriggered,
		strings.ReplaceAll(reading.Category, `"`, `\"`),
		reading.CreatedAt.UnixNano(),
	)

	endpoint, err := url.Parse(s.url + "/api/v2/write")
	if err != nil {
		return err
	}
	query := endpoint.Query()
	query.Set("org", s.org)
	query.Set("bucket", s.bucket)
	query.Set("precision", "ns")
	endpoint.RawQuery = query.Encode()

	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), bytes.NewBufferString(line))
	if err != nil {
		return err
	}
	request.Header.Set("authorization", "Token "+s.token)
	request.Header.Set("content-type", "text/plain")

	response, err := s.client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("influx write failed with status %d", response.StatusCode)
	}

	return nil
}

type FanoutReadingStore struct {
	primary ReadingStore
	influx  *InfluxWriter
}

func NewFanoutReadingStore(primary ReadingStore, influx *InfluxWriter) *FanoutReadingStore {
	return &FanoutReadingStore{primary: primary, influx: influx}
}

func (s *FanoutReadingStore) CreateReading(ctx context.Context, reading Reading) (Reading, error) {
	stored, err := s.primary.CreateReading(ctx, reading)
	if err != nil {
		return Reading{}, err
	}
	if err := s.influx.WriteReading(ctx, stored); err != nil {
		return Reading{}, err
	}
	return stored, nil
}

func (s *FanoutReadingStore) ListReadings(ctx context.Context, filter ReadingFilter) ([]Reading, error) {
	return s.primary.ListReadings(ctx, filter)
}

func (s *FanoutReadingStore) Mode() string {
	return s.primary.Mode() + "+influxdb"
}

type API struct {
	users    UserStore
	readings ReadingStore
}

func NewAPI(users UserStore, readings ReadingStore) *API {
	return &API{users: users, readings: readings}
}

func (api *API) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", method(http.MethodGet, api.health))
	mux.HandleFunc("/metrics", method(http.MethodGet, api.metrics))
	mux.HandleFunc("/users", method(http.MethodPost, api.createUser))
	mux.HandleFunc("/users/", method(http.MethodGet, api.getUser))
	mux.HandleFunc("/readings", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			api.listReadings(w, r)
		case http.MethodPost:
			api.createReading(w, r)
		default:
			writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		}
	})
	return mux
}

func method(expected string, handler http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != expected {
			writeError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		handler(w, r)
	}
}

func (api *API) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{
		"status":        "ok",
		"service":       "io-service",
		"user_store":    api.users.Mode(),
		"reading_store": api.readings.Mode(),
	})
}

func (api *API) metrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("content-type", "text/plain")
	_, _ = w.Write([]byte("airpure_io_service_up 1\n"))
}

func (api *API) createUser(w http.ResponseWriter, r *http.Request) {
	var payload struct {
		Username       string   `json:"username"`
		PasswordHash   string   `json:"password_hash"`
		Role           string   `json:"role"`
		AlertThreshold *float64 `json:"alert_threshold"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON payload")
		return
	}

	payload.Username = strings.TrimSpace(payload.Username)
	alertThreshold := 100.0
	if payload.AlertThreshold != nil {
		alertThreshold = *payload.AlertThreshold
	}
	if !validUsername(payload.Username) || payload.PasswordHash == "" || alertThreshold <= 0 {
		writeError(w, http.StatusBadRequest, "username, password_hash, and a positive alert_threshold are required")
		return
	}

	user, err := api.users.CreateUser(r.Context(), User{
		Username:       payload.Username,
		PasswordHash:   payload.PasswordHash,
		Role:           normalizeRole(payload.Role),
		AlertThreshold: alertThreshold,
	})
	if errors.Is(err, ErrConflict) {
		writeError(w, http.StatusConflict, "user already exists")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusCreated, user)
}

func (api *API) getUser(w http.ResponseWriter, r *http.Request) {
	username := strings.TrimPrefix(r.URL.Path, "/users/")
	if username == "" {
		writeError(w, http.StatusBadRequest, "username is required")
		return
	}
	user, err := api.users.FindUser(r.Context(), username)
	if errors.Is(err, ErrNotFound) {
		writeError(w, http.StatusNotFound, "user not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, user)
}

func (api *API) createReading(w http.ResponseWriter, r *http.Request) {
	var payload struct {
		PM25           float64 `json:"pm25"`
		CO2            float64 `json:"co2"`
		AQI            float64 `json:"aqi"`
		Category       string  `json:"category"`
		Location       string  `json:"location"`
		Source         string  `json:"source"`
		UserID         string  `json:"user_id"`
		AlertThreshold float64 `json:"alert_threshold"`
		AlertTriggered bool    `json:"alert_triggered"`
		AlertMessage   string  `json:"alert_message"`
		ProcessedAt    string  `json:"processed_at"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON payload")
		return
	}

	if payload.PM25 < 0 || payload.CO2 < 0 || payload.AQI < 0 || payload.AlertThreshold < 0 {
		writeError(w, http.StatusBadRequest, "pm25, co2, aqi, and alert_threshold must be non-negative")
		return
	}

	processedAt := time.Now().UTC()
	if payload.ProcessedAt != "" {
		parsed, err := time.Parse(time.RFC3339, payload.ProcessedAt)
		if err != nil {
			writeError(w, http.StatusBadRequest, "processed_at must be RFC3339")
			return
		}
		processedAt = parsed
	}

	reading, err := api.readings.CreateReading(r.Context(), Reading{
		PM25:           payload.PM25,
		CO2:            payload.CO2,
		AQI:            payload.AQI,
		Category:       defaultString(payload.Category, "unknown"),
		Location:       defaultString(payload.Location, "unknown"),
		Source:         defaultString(payload.Source, "api"),
		UserID:         payload.UserID,
		AlertThreshold: defaultFloat(payload.AlertThreshold, 100),
		AlertTriggered: payload.AlertTriggered,
		AlertMessage:   payload.AlertMessage,
		ProcessedAt:    processedAt,
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusCreated, reading)
}

func (api *API) listReadings(w http.ResponseWriter, r *http.Request) {
	limit, err := strconv.Atoi(defaultString(r.URL.Query().Get("limit"), "50"))
	if err != nil || limit <= 0 || limit > 500 {
		writeError(w, http.StatusBadRequest, "limit must be between 1 and 500")
		return
	}

	readings, err := api.readings.ListReadings(r.Context(), ReadingFilter{
		UserID: r.URL.Query().Get("user_id"),
		Limit:  limit,
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string][]Reading{"readings": readings})
}

func validUsername(username string) bool {
	if len(username) < 3 || len(username) > 64 {
		return false
	}
	for _, char := range username {
		if (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') || (char >= '0' && char <= '9') || char == '_' || char == '-' || char == '.' {
			continue
		}
		return false
	}
	return true
}

func normalizeRole(value string) string {
	if value == "admin" {
		return "admin"
	}
	return "user"
}

func defaultString(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}

func defaultFloat(value, fallback float64) float64 {
	if value == 0 {
		return fallback
	}
	return value
}

func normalizeLimit(limit int) int {
	if limit <= 0 {
		return 50
	}
	if limit > 500 {
		return 500
	}
	return limit
}

func escapeInfluxTag(value string) string {
	replacer := strings.NewReplacer(",", `\,`, " ", `\ `, "=", `\=`)
	return replacer.Replace(defaultString(value, "unknown"))
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("content-type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

func buildStores(ctx context.Context) (UserStore, ReadingStore, error) {
	userStore := UserStore(NewMemoryUserStore())
	readingStore := ReadingStore(NewMemoryReadingStore())

	if databaseURL := os.Getenv("POSTGRES_URL"); databaseURL != "" {
		store, err := connectPostgresWithRetry(ctx, databaseURL)
		if err != nil {
			return nil, nil, err
		}
		userStore = store
		readingStore = store
	}

	influxURL := os.Getenv("INFLUX_URL")
	influxOrg := os.Getenv("INFLUX_ORG")
	influxBucket := os.Getenv("INFLUX_BUCKET")
	influxToken := os.Getenv("INFLUX_TOKEN")
	if influxURL != "" || influxOrg != "" || influxBucket != "" || influxToken != "" {
		if influxURL == "" || influxOrg == "" || influxBucket == "" || influxToken == "" {
			return nil, nil, errors.New("INFLUX_URL, INFLUX_ORG, INFLUX_BUCKET, and INFLUX_TOKEN must be set together")
		}
		if err := waitForInflux(ctx, influxURL); err != nil {
			return nil, nil, fmt.Errorf("influxdb is required but unavailable: %w", err)
		}
		readingStore = NewFanoutReadingStore(readingStore, NewInfluxWriter(influxURL, influxOrg, influxBucket, influxToken))
	}

	return userStore, readingStore, nil
}

func connectPostgresWithRetry(ctx context.Context, databaseURL string) (*PostgresStore, error) {
	retries := envInt("DB_CONNECT_RETRIES", 10)
	delay := time.Duration(envInt("DB_RETRY_DELAY_MS", 2000)) * time.Millisecond
	var lastErr error

	for attempt := 1; attempt <= retries; attempt++ {
		store, err := NewPostgresStore(ctx, databaseURL)
		if err == nil {
			return store, nil
		}
		lastErr = err
		log.Printf("postgres connection attempt %d/%d failed: %v", attempt, retries, err)

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(delay):
		}
	}

	return nil, fmt.Errorf("postgres is required but unavailable: %w", lastErr)
}

func waitForInflux(ctx context.Context, baseURL string) error {
	retries := envInt("INFLUX_CONNECT_RETRIES", envInt("DB_CONNECT_RETRIES", 10))
	delay := time.Duration(envInt("INFLUX_RETRY_DELAY_MS", envInt("DB_RETRY_DELAY_MS", 2000))) * time.Millisecond
	client := &http.Client{Timeout: 2 * time.Second}
	healthURL := strings.TrimRight(baseURL, "/") + "/health"
	var lastErr error

	for attempt := 1; attempt <= retries; attempt++ {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, healthURL, nil)
		if err != nil {
			return err
		}

		response, err := client.Do(request)
		if err == nil {
			response.Body.Close()
			if response.StatusCode >= 200 && response.StatusCode < 400 {
				return nil
			}
			err = fmt.Errorf("health returned status %d", response.StatusCode)
		}
		lastErr = err
		log.Printf("influxdb connection attempt %d/%d failed: %v", attempt, retries, err)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}

	return lastErr
}

func envInt(name string, fallback int) int {
	value, err := strconv.Atoi(os.Getenv(name))
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	userStore, readingStore, err := buildStores(ctx)
	if err != nil {
		log.Fatal(err)
	}

	port := 7000
	if rawPort := os.Getenv("PORT"); rawPort != "" {
		if parsed, err := strconv.Atoi(rawPort); err == nil {
			port = parsed
		}
	}

	server := &http.Server{
		Addr:              fmt.Sprintf(":%d", port),
		Handler:           NewAPI(userStore, readingStore).routes(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	log.Printf("IO Service running on port %d", port)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}
