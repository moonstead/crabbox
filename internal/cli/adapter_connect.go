package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"nhooyr.io/websocket"
)

const (
	adapterRelayMaxBodyBytes          = 64 << 10
	adapterRelayMaxMessageBytes       = adapterRelayMaxBodyBytes*6 + (16 << 10)
	adapterRelayRequestTimeout        = 9 * time.Second
	adapterRelayHandshakeTimeout      = 10 * time.Second
	adapterRelayDefaultConnectionTime = 2 * time.Minute
	adapterRelayMaxConnectionTime     = 24 * time.Hour
	adapterRelayConnectionOverhead    = 30 * time.Second
	adapterRelayWriteTimeout          = 5 * time.Second
	adapterRelayMinBackoff            = 250 * time.Millisecond
	adapterRelayMaxBackoff            = 5 * time.Second
	adapterRelayMaxConcurrentRequests = 64
	adapterRelayMaxConcurrentDeletes  = 8
	// Workspace exec is opt-in. Its body carries private stdin, so it has its
	// own bound and lane, and a timeout covering the command plus SSH setup.
	adapterRelayMaxExecBodyBytes    = controllerExecMaxBodyBytes
	adapterRelayMaxExecMessageBytes = adapterRelayMaxExecBodyBytes*6 + (16 << 10)
	adapterRelayExecOverhead        = 30 * time.Second
	adapterRelayMaxConcurrentExecs  = 4
)

type coordinatorAdapterTicket struct {
	Ticket    string `json:"ticket"`
	AdapterID string `json:"adapterID,omitempty"`
	ExpiresAt string `json:"expiresAt,omitempty"`
}

type adapterRelayRequest struct {
	Type       string            `json:"type"`
	ID         string            `json:"id"`
	Method     string            `json:"method"`
	Path       string            `json:"path"`
	DeadlineMS int64             `json:"deadlineMs"`
	Headers    map[string]string `json:"headers,omitempty"`
	Body       *string           `json:"body,omitempty"`
}

type adapterRelayResponse struct {
	Type    string            `json:"type"`
	ID      string            `json:"id"`
	Status  int               `json:"status"`
	Headers map[string]string `json:"headers,omitempty"`
	Body    string            `json:"body,omitempty"`
}

type adapterRelay struct {
	localBaseURL   string
	loadLocalToken func() (string, error)
	client         *http.Client
	desktopTimeout time.Duration
	// execTimeout is zero unless this connector relays workspace exec.
	execTimeout time.Duration
	ws          *websocket.Conn
	writeMu     sync.Mutex
	inflightMu  sync.Mutex
	inflight    map[string]context.CancelFunc
}

func (a App) adapterConnect(ctx context.Context, args []string) error {
	if err := adapterConnectHostSupported(); err != nil {
		return Exit(2, "%v", err)
	}
	fs := newFlagSet("adapter connect", a.Stderr)
	id := fs.String("id", getenv("CRABBOX_ADAPTER_ID", ""), "coordinator adapter id")
	localSocket := fs.String("local-socket", getenv("CRABBOX_ADAPTER_LOCAL_SOCKET", ""), "current-user-owned local adapter Unix socket (required)")
	tokenFile := fs.String("token-file", getenv("CRABBOX_ADAPTER_TOKEN_FILE", ""), "file containing the local adapter bearer token (required)")
	connectionTimeout := fs.Duration("connection-timeout", controllerEnvDuration("CRABBOX_ADAPTER_CONNECTION_TIMEOUT", adapterRelayDefaultConnectionTime), "local adapter desktop connection setup duration")
	allowExec := fs.Bool("allow-exec", controllerEnvBool("CRABBOX_ADAPTER_ALLOW_EXEC"), "relay workspace exec requests to the local adapter")
	execTimeout := fs.Duration("exec-timeout", controllerEnvDuration("CRABBOX_ADAPTER_EXEC_TIMEOUT", controllerExecDefaultMaxTimeout), "local adapter workspace exec duration; match adapter serve --exec-max-timeout")
	if err := parseFlags(fs, args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return Exit(2, "usage: crabbox adapter connect --id <adapter-id> --local-socket <path> --token-file <path> [--connection-timeout <duration>] [--allow-exec [--exec-timeout <duration>]]")
	}
	adapterID := strings.TrimSpace(*id)
	if !validControllerWorkspaceID(adapterID) {
		return Exit(2, "--id must be a lowercase DNS-style name with at most 63 characters")
	}
	if strings.TrimSpace(*tokenFile) == "" {
		return Exit(2, "--token-file is required")
	}
	if *connectionTimeout <= 0 {
		return Exit(2, "--connection-timeout must be greater than zero")
	}
	if *connectionTimeout > adapterRelayMaxConnectionTime {
		return Exit(2, "--connection-timeout must not exceed %s", adapterRelayMaxConnectionTime)
	}
	desktopRequestTimeout := *connectionTimeout + adapterRelayConnectionOverhead
	if desktopRequestTimeout <= *connectionTimeout {
		return Exit(2, "--connection-timeout is too large")
	}
	execRequestTimeout := time.Duration(0)
	if *allowExec {
		if *execTimeout < controllerExecMinTimeout || *execTimeout > controllerExecMaximumTimeout {
			return Exit(2, "--exec-timeout must be between %s and %s", controllerExecMinTimeout, controllerExecMaximumTimeout)
		}
		execRequestTimeout = *execTimeout + adapterRelayExecOverhead
	}
	socketPath, err := normalizeAdapterUnixSocketPath(strings.TrimSpace(*localSocket))
	if err != nil {
		return Exit(2, "--local-socket: %v", err)
	}
	loadLocalToken := func() (string, error) {
		return readAdapterToken(*tokenFile)
	}
	if _, err := loadLocalToken(); err != nil {
		return err
	}
	localClient, err := newAdapterLocalClient(socketPath, max(desktopRequestTimeout, execRequestTimeout))
	if err != nil {
		return Exit(2, "--local-socket: %v", err)
	}
	if _, err := configuredAdapterCoordinatorClient(); err != nil {
		return err
	}

	defer localClient.CloseIdleConnections()
	return runAdapterRelayLoop(ctx, a.Stderr, func(connectCtx context.Context) error {
		// Reload the normal Crabbox config before every ticket. A long-running
		// relay therefore picks up a refreshed coordinator session token without
		// persisting a second credential.
		coord, err := configuredAdapterCoordinatorClient()
		if err != nil {
			return err
		}
		if _, err := loadLocalToken(); err != nil {
			return err
		}
		return connectAdapterRelay(connectCtx, coord, adapterID, "http://adapter.local", socketPath, loadLocalToken, localClient, desktopRequestTimeout, execRequestTimeout, a.Stdout)
	})
}

func configuredAdapterCoordinatorClient() (*CoordinatorClient, error) {
	cfg, err := loadConfig()
	if err != nil {
		return nil, err
	}
	coord, configured, err := newCoordinatorClient(cfg)
	if err != nil {
		return nil, err
	}
	if !configured || coord == nil || strings.TrimSpace(coord.BaseURL) == "" {
		return nil, Exit(2, "adapter connect requires a configured coordinator; run crabbox login --url <coordinator-url> first")
	}
	if !coord.hasConfiguredAuth() {
		return nil, Exit(2, "adapter connect requires coordinator authentication; run crabbox login --url <coordinator-url> first")
	}
	return coord, nil
}

func runAdapterRelayLoop(ctx context.Context, log io.Writer, connect func(context.Context) error) error {
	for attempt := 0; ; attempt++ {
		err := connect(ctx)
		if ctx.Err() != nil {
			return nil
		}
		delay := adapterRelayBackoff(attempt)
		if log != nil {
			if err == nil {
				fmt.Fprintf(log, "adapter relay disconnected; reconnecting in %s\n", delay)
			} else {
				fmt.Fprintf(log, "adapter relay disconnected: %v; reconnecting in %s\n", err, delay)
			}
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return nil
		case <-timer.C:
		}
	}
}

func adapterRelayBackoff(attempt int) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	delay := adapterRelayMinBackoff
	for i := 0; i < attempt && delay < adapterRelayMaxBackoff; i++ {
		delay *= 2
		if delay >= adapterRelayMaxBackoff {
			return adapterRelayMaxBackoff
		}
	}
	return delay
}

func connectAdapterRelay(
	ctx context.Context,
	coord *CoordinatorClient,
	adapterID string,
	localBaseURL string,
	localSocket string,
	loadLocalToken func() (string, error),
	localClient *http.Client,
	desktopRequestTimeout time.Duration,
	execRequestTimeout time.Duration,
	status io.Writer,
) error {
	dialCtx, cancelDial := context.WithTimeout(ctx, adapterRelayHandshakeTimeout)
	defer cancelDial()
	coordinatorDesktopTimeout := desktopRequestTimeout + adapterRelayWriteTimeout
	if coordinatorDesktopTimeout <= desktopRequestTimeout {
		return errors.New("adapter relay desktop timeout is too large")
	}
	// Advertising an exec budget is this connector's opt-in; a coordinator
	// never relays exec to a connector that did not advertise one.
	coordinatorExecTimeout := time.Duration(0)
	if execRequestTimeout > 0 {
		coordinatorExecTimeout = execRequestTimeout + adapterRelayWriteTimeout
	}
	ticket, err := coord.CreateAdapterTicket(dialCtx, adapterID, coordinatorDesktopTimeout, coordinatorExecTimeout)
	if err != nil {
		return fmt.Errorf("create adapter ticket: %w", err)
	}
	if strings.TrimSpace(ticket.Ticket) == "" {
		return errors.New("coordinator returned an empty adapter ticket")
	}
	headers := bridgeTicketHeaders(coord, ticket.Ticket)
	dialOptions := &websocket.DialOptions{
		HTTPHeader: headers,
	}
	if coord.Client != nil {
		dialOptions.HTTPClient = coord.Client
	}
	ws, response, err := websocket.Dial(dialCtx, adapterRelayAgentURL(coord.BaseURL, adapterID), dialOptions)
	if err != nil {
		if response != nil && response.Body != nil {
			_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 8<<10))
			_ = response.Body.Close()
		}
		return fmt.Errorf("connect adapter relay: %w", err)
	}
	readLimit := int64(adapterRelayMaxMessageBytes)
	if execRequestTimeout > 0 {
		readLimit = adapterRelayMaxExecMessageBytes
	}
	ws.SetReadLimit(readLimit)
	relay := &adapterRelay{
		localBaseURL:   localBaseURL,
		loadLocalToken: loadLocalToken,
		client:         localClient,
		desktopTimeout: desktopRequestTimeout,
		execTimeout:    execRequestTimeout,
		ws:             ws,
		inflight:       map[string]context.CancelFunc{},
	}
	defer ws.Close(websocket.StatusNormalClosure, "adapter relay stopped")
	if status != nil {
		fmt.Fprintf(status, "adapter relay connected id=%s coordinator=%s local_socket=%s\n", adapterID, redactedConfigURL(coord.BaseURL), localSocket)
	}
	return relay.serve(ctx)
}

func (c *CoordinatorClient) CreateAdapterTicket(ctx context.Context, adapterID string, desktopTimeout, execTimeout time.Duration) (coordinatorAdapterTicket, error) {
	var result coordinatorAdapterTicket
	body := map[string]any{
		"desktopTimeoutMs": desktopTimeout.Milliseconds(),
	}
	if execTimeout > 0 {
		body["execTimeoutMs"] = execTimeout.Milliseconds()
	}
	err := c.do(ctx, http.MethodPost, "/v1/adapters/"+url.PathEscape(adapterID)+"/ticket", body, &result)
	return result, err
}

func adapterRelayAgentURL(baseURL, adapterID string) string {
	u, err := url.Parse(baseURL)
	if err != nil {
		return baseURL
	}
	if u.Scheme == "https" {
		u.Scheme = "wss"
	} else {
		u.Scheme = "ws"
	}
	u.Path = strings.TrimRight(u.Path, "/") + "/v1/adapters/" + url.PathEscape(adapterID) + "/agent"
	u.RawQuery = ""
	u.Fragment = ""
	return u.String()
}

func (r *adapterRelay) serve(ctx context.Context) error {
	relayCtx, cancel := context.WithCancelCause(ctx)
	requests := make(chan struct{}, adapterRelayMaxConcurrentRequests)
	deletes := make(chan struct{}, adapterRelayMaxConcurrentDeletes)
	execs := make(chan struct{}, adapterRelayMaxConcurrentExecs)
	var workers sync.WaitGroup
	defer func() {
		cancel(context.Canceled)
		workers.Wait()
	}()
	for {
		messageType, data, err := r.ws.Read(relayCtx)
		if err != nil {
			if cause := context.Cause(relayCtx); cause != nil && !errors.Is(cause, context.Canceled) {
				return cause
			}
			return err
		}
		if messageType != websocket.MessageText {
			return errors.New("adapter relay accepts text messages only")
		}
		request, err := decodeAdapterRelayRequest(data)
		if err != nil {
			return fmt.Errorf("decode adapter relay request: %w", err)
		}
		if request.Type == "cancel" {
			// The coordinator withdrew a request whose caller went away or whose
			// deadline passed. Unknown IDs have already finished.
			if r.execTimeout > 0 && validAdapterRelayRequestID(request.ID) {
				r.cancelInflight(request.ID)
			}
			continue
		}
		limit := requests
		if request.Method == http.MethodDelete {
			// Keep cancellation available even while slow desktop setup requests
			// occupy every ordinary relay slot.
			limit = deletes
		} else if adapterRelayExecPath(request.Method, request.Path) {
			limit = execs
		}
		select {
		case limit <- struct{}{}:
			requestCtx, cancelRequest := context.WithCancel(relayCtx)
			if !r.trackInflight(request.ID, cancelRequest) {
				cancelRequest()
				<-limit
				response := adapterRelayErrorResponse(request.ID, http.StatusConflict, "duplicate_request", "adapter relay request id is already in flight")
				if err := r.writeResponse(relayCtx, response); err != nil {
					cancel(err)
					return err
				}
				continue
			}
			workers.Add(1)
			go func() {
				defer workers.Done()
				defer func() { <-limit }()
				defer r.finishInflight(request.ID, cancelRequest)
				response := r.handle(requestCtx, request)
				if err := r.writeResponse(relayCtx, response); err != nil {
					cancel(err)
				}
			}()
		default:
			response := adapterRelayErrorResponse(request.ID, http.StatusTooManyRequests, "adapter_busy", "adapter relay concurrency limit reached")
			if err := r.writeResponse(relayCtx, response); err != nil {
				cancel(err)
				return err
			}
		}
	}
}

func (r *adapterRelay) writeResponse(ctx context.Context, response adapterRelayResponse) error {
	data, err := json.Marshal(response)
	if err != nil {
		return err
	}
	writeCtx, cancel := context.WithTimeout(ctx, adapterRelayWriteTimeout)
	defer cancel()
	r.writeMu.Lock()
	defer r.writeMu.Unlock()
	return r.ws.Write(writeCtx, websocket.MessageText, data)
}

func decodeAdapterRelayRequest(data []byte) (adapterRelayRequest, error) {
	var request adapterRelayRequest
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		return request, err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			return request, errors.New("request must contain one JSON object")
		}
		return request, err
	}
	return request, nil
}

func (r *adapterRelay) trackInflight(id string, cancel context.CancelFunc) bool {
	r.inflightMu.Lock()
	defer r.inflightMu.Unlock()
	if _, exists := r.inflight[id]; exists {
		return false
	}
	r.inflight[id] = cancel
	return true
}

func (r *adapterRelay) finishInflight(id string, cancel context.CancelFunc) {
	cancel()
	r.inflightMu.Lock()
	delete(r.inflight, id)
	r.inflightMu.Unlock()
}

func (r *adapterRelay) cancelInflight(id string) {
	r.inflightMu.Lock()
	cancel := r.inflight[id]
	r.inflightMu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (r *adapterRelay) handle(ctx context.Context, request adapterRelayRequest) adapterRelayResponse {
	if err := validateAdapterRelayRequestWithExec(request, r.execTimeout > 0); err != nil {
		return adapterRelayErrorResponse(request.ID, http.StatusBadRequest, "invalid_request", err.Error())
	}
	deadline := time.UnixMilli(request.DeadlineMS)
	if !deadline.After(time.Now()) {
		return adapterRelayErrorResponse(request.ID, http.StatusGatewayTimeout, "adapter_timeout", "adapter relay request expired before local dispatch")
	}
	body := []byte(nil)
	if request.Body != nil {
		body = []byte(*request.Body)
	}
	deadlineCtx, cancelDeadline := context.WithDeadline(ctx, deadline)
	defer cancelDeadline()
	requestCtx, cancelTimeout := context.WithTimeout(deadlineCtx, adapterRelayTimeout(request, r.desktopTimeout, r.execTimeout))
	defer cancelTimeout()
	localPath, ok := adapterRelayCanonicalPath(request.Method, request.Path)
	if r.execTimeout > 0 && adapterRelayExecPath(request.Method, request.Path) {
		localPath, ok = request.Path, true
	}
	if !ok {
		return adapterRelayErrorResponse(request.ID, http.StatusBadRequest, "invalid_request", "method and path are outside the crabfleet/v1 adapter surface")
	}
	localRequest, err := http.NewRequestWithContext(requestCtx, request.Method, r.localBaseURL+localPath, bytes.NewReader(body))
	if err != nil {
		return adapterRelayErrorResponse(request.ID, http.StatusBadRequest, "invalid_request", "could not construct local adapter request")
	}
	for key, value := range request.Headers {
		switch strings.ToLower(strings.TrimSpace(key)) {
		case "accept":
			localRequest.Header.Set("Accept", value)
		case "content-type":
			localRequest.Header.Set("Content-Type", value)
		case "idempotency-key":
			localRequest.Header.Set("Idempotency-Key", value)
		}
	}
	localToken, err := r.loadLocalToken()
	if err != nil {
		return adapterRelayErrorResponse(request.ID, http.StatusBadGateway, "adapter_auth_unavailable", "local adapter token could not be loaded")
	}
	localRequest.Header.Set("Authorization", "Bearer "+localToken)
	localRequest.Header.Set("Accept", "application/json")
	if request.Method == http.MethodPost && (request.Path == "/v1/workspaces" || adapterRelayExecPath(request.Method, request.Path)) && localRequest.Header.Get("Content-Type") == "" {
		localRequest.Header.Set("Content-Type", "application/json")
	}
	if requestCtx.Err() != nil {
		return adapterRelayErrorResponse(request.ID, http.StatusGatewayTimeout, "adapter_timeout", "adapter relay request expired before local dispatch")
	}

	localResponse, err := r.client.Do(localRequest)
	if err != nil {
		if requestCtx.Err() != nil && ctx.Err() == nil {
			return adapterRelayErrorResponse(request.ID, http.StatusGatewayTimeout, "adapter_timeout", "local adapter request timed out")
		}
		return adapterRelayErrorResponse(request.ID, http.StatusBadGateway, "adapter_unavailable", "local adapter request failed")
	}
	defer localResponse.Body.Close()
	responseBody, err := readAdapterRelayResponseBody(localResponse.Body)
	if err != nil {
		return adapterRelayErrorResponse(request.ID, http.StatusBadGateway, "invalid_adapter_response", err.Error())
	}
	response := adapterRelayResponse{
		Type:   "response",
		ID:     request.ID,
		Status: localResponse.StatusCode,
		Body:   responseBody,
	}
	if contentType := strings.TrimSpace(localResponse.Header.Get("Content-Type")); contentType != "" {
		response.Headers = map[string]string{"content-type": contentType}
	}
	return response
}

func adapterRelayTimeout(request adapterRelayRequest, desktopTimeout, execTimeout time.Duration) time.Duration {
	if execTimeout > 0 && adapterRelayExecPath(request.Method, request.Path) {
		return execTimeout
	}
	return adapterRelayTimeoutForRequest(request, desktopTimeout)
}

func adapterRelayTimeoutForRequest(request adapterRelayRequest, desktopTimeout time.Duration) time.Duration {
	if request.Method == http.MethodPost && (strings.HasSuffix(request.Path, "/connections/desktop") || strings.HasSuffix(request.Path, "/connections/native-vnc")) {
		if desktopTimeout <= 0 {
			desktopTimeout = adapterRelayDefaultConnectionTime + adapterRelayConnectionOverhead
		}
		return desktopTimeout
	}
	return adapterRelayRequestTimeout
}

func validateAdapterRelayRequest(request adapterRelayRequest) error {
	return validateAdapterRelayRequestWithExec(request, false)
}

func validateAdapterRelayRequestWithExec(request adapterRelayRequest, allowExec bool) error {
	if request.Type != "request" {
		return errors.New("type must be request")
	}
	if !validAdapterRelayRequestID(request.ID) {
		return errors.New("id must be 1 to 128 printable characters")
	}
	if request.DeadlineMS <= 0 {
		return errors.New("deadlineMs must be a positive Unix millisecond timestamp")
	}
	if adapterRelayExecPath(request.Method, request.Path) {
		if !allowExec {
			return errors.New("workspace exec is not enabled on this adapter relay")
		}
		if err := validateAdapterRelayExecBody(request.Body); err != nil {
			return err
		}
		return validateAdapterRelayHeaders(request.Headers)
	}
	if request.Method != strings.ToUpper(request.Method) || !adapterRelayRouteAllowed(request.Method, request.Path) {
		return errors.New("method and path are outside the crabfleet/v1 adapter surface")
	}
	if request.Body != nil && len([]byte(*request.Body)) > adapterRelayMaxBodyBytes {
		return errors.New("body exceeds 64 KiB")
	}
	if request.Body != nil && *request.Body != "" && request.Method != http.MethodPost {
		return errors.New("request body is not allowed for this method")
	}
	if request.Body != nil && *request.Body != "" && request.Path != "/v1/workspaces" {
		return errors.New("request body is allowed only for workspace creation")
	}
	if err := validateAdapterRelayHeaders(request.Headers); err != nil {
		return err
	}
	return nil
}

func validAdapterRelayRequestID(value string) bool {
	if len(value) < 1 || len(value) > 128 {
		return false
	}
	for _, character := range value {
		if character < 0x20 || character == 0x7f {
			return false
		}
	}
	return true
}

// adapterRelayExecPath reports POST /v1/workspaces/{id}/exec. It is kept out
// of adapterRelayCanonicalPath so the ordinary surface stays unchanged.
func adapterRelayExecPath(method, requestPath string) bool {
	const prefix, suffix = "/v1/workspaces/", "/exec"
	if method != http.MethodPost || !strings.HasPrefix(requestPath, prefix) || !strings.HasSuffix(requestPath, suffix) || strings.ContainsAny(requestPath, "?#%") {
		return false
	}
	return validControllerWorkspaceID(strings.TrimSuffix(strings.TrimPrefix(requestPath, prefix), suffix))
}

// validateAdapterRelayExecBody requires the coordinator's execution-time
// binding. The body also carries private stdin: never quote it in errors.
func validateAdapterRelayExecBody(body *string) error {
	if body == nil || *body == "" {
		return errors.New("workspace exec requires a request body")
	}
	if len(*body) > adapterRelayMaxExecBodyBytes {
		return errors.New("workspace exec body exceeds its limit")
	}
	var binding struct {
		LeaseID        string `json:"leaseId"`
		RegistrationID string `json:"registrationId"`
	}
	if json.Unmarshal([]byte(*body), &binding) != nil {
		return errors.New("workspace exec body must be a JSON object")
	}
	if !IsCanonicalLeaseID(binding.LeaseID) || !validControllerWorkspaceID(binding.RegistrationID) {
		return errors.New("workspace exec requires the coordinator's lease and registration binding")
	}
	return nil
}

func adapterRelayRouteAllowed(method, requestPath string) bool {
	_, ok := adapterRelayCanonicalPath(method, requestPath)
	return ok
}

func adapterRelayCanonicalPath(method, requestPath string) (string, bool) {
	if requestPath == "/v1/workspaces" {
		return requestPath, method == http.MethodPost
	}
	const prefix = "/v1/workspaces/"
	if !strings.HasPrefix(requestPath, prefix) || strings.ContainsAny(requestPath, "?#%") {
		return "", false
	}
	rest := strings.TrimPrefix(requestPath, prefix)
	if method == http.MethodPost && (strings.HasSuffix(rest, "/connections/desktop") || strings.HasSuffix(rest, "/connections/native-vnc")) {
		suffix := "/connections/desktop"
		if strings.HasSuffix(rest, "/connections/native-vnc") {
			suffix = "/connections/native-vnc"
		}
		workspaceID := strings.TrimSuffix(rest, suffix)
		if !validControllerWorkspaceID(workspaceID) {
			return "", false
		}
		return prefix + url.PathEscape(workspaceID) + suffix, true
	}
	if (method != http.MethodGet && method != http.MethodDelete) || !validControllerWorkspaceID(rest) {
		return "", false
	}
	return prefix + url.PathEscape(rest), true
}

func validateAdapterRelayHeaders(headers map[string]string) error {
	if len(headers) > 16 {
		return errors.New("too many request headers")
	}
	total := 0
	for key, value := range headers {
		if len(key) == 0 || len(key) > 64 || len(value) > 4096 || strings.ContainsAny(key, "\r\n") || strings.ContainsAny(value, "\r\n") {
			return errors.New("request header is invalid")
		}
		total += len(key) + len(value)
	}
	if total > 8<<10 {
		return errors.New("request headers exceed 8 KiB")
	}
	return nil
}

func readAdapterRelayResponseBody(body io.Reader) (string, error) {
	data, err := io.ReadAll(io.LimitReader(body, adapterRelayMaxBodyBytes+1))
	if err != nil {
		return "", errors.New("could not read local adapter response")
	}
	if len(data) > adapterRelayMaxBodyBytes {
		return "", errors.New("local adapter response exceeds 64 KiB")
	}
	if !utf8.Valid(data) {
		return "", errors.New("local adapter response is not UTF-8")
	}
	return string(data), nil
}

func adapterRelayErrorResponse(id string, status int, code, message string) adapterRelayResponse {
	if !validAdapterRelayRequestID(id) {
		id = ""
	}
	body, _ := json.Marshal(map[string]any{
		"error": map[string]string{
			"code":    code,
			"message": message,
		},
	})
	return adapterRelayResponse{
		Type:    "response",
		ID:      id,
		Status:  status,
		Headers: map[string]string{"content-type": "application/json; charset=utf-8"},
		Body:    string(body),
	}
}
