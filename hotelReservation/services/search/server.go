package search

import (
	// "encoding/json"
	"fmt"
	"math"
	"math/rand"
	// F"io/ioutil"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync/atomic"

	"github.com/rs/zerolog/log"

	// "os"
	"time"

	"github.com/google/uuid"
	"github.com/grpc-ecosystem/grpc-opentracing/go/otgrpc"
	"github.com/harlow/go-micro-services/dialer"
	"github.com/harlow/go-micro-services/registry"
	geo "github.com/harlow/go-micro-services/services/geo/proto"
	rate "github.com/harlow/go-micro-services/services/rate/proto"
	pb "github.com/harlow/go-micro-services/services/search/proto"
	"github.com/harlow/go-micro-services/tls"
	opentracing "github.com/opentracing/opentracing-go"
	context "golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
)

const name = "srv-search"

type rateRetryPolicy struct {
	timeout           time.Duration
	maxAttempts       int
	initialBackoff    time.Duration
	backoffMultiplier float64
	jitterFraction    float64
}

type searchMetrics struct {
	requestsTotal      uint64
	rateAttemptsTotal  uint64
	rateTimeoutsTotal  uint64
	rateFailuresTotal  uint64
	rateSuccessesTotal uint64
}

// Server implments the search service
type Server struct {
	geoClient  geo.GeoClient
	rateClient rate.RateClient

	Tracer      opentracing.Tracer
	Port        int
	IpAddr      string
	KnativeDns  string
	Registry    *registry.Client
	uuid        string
	retryPolicy rateRetryPolicy
	metrics     searchMetrics
}

func envInt(name string, fallback int) int {
	value, err := strconv.Atoi(os.Getenv(name))
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}

func envFloat(name string, fallback float64) float64 {
	value, err := strconv.ParseFloat(os.Getenv(name), 64)
	if err != nil || value < 0 {
		return fallback
	}
	return value
}

func loadRateRetryPolicy() rateRetryPolicy {
	return rateRetryPolicy{
		timeout:           time.Duration(envInt("RATE_RPC_TIMEOUT_MS", 750)) * time.Millisecond,
		maxAttempts:       envInt("RATE_RPC_MAX_ATTEMPTS", 3),
		initialBackoff:    time.Duration(envInt("RATE_RPC_INITIAL_BACKOFF_MS", 50)) * time.Millisecond,
		backoffMultiplier: envFloat("RATE_RPC_BACKOFF_MULTIPLIER", 2.0),
		jitterFraction:    envFloat("RATE_RPC_JITTER", 0.2),
	}
}

func (p rateRetryPolicy) backoff(attempt int) time.Duration {
	delay := float64(p.initialBackoff) * math.Pow(p.backoffMultiplier, float64(attempt-1))
	if p.jitterFraction > 0 {
		delay *= 1 + ((rand.Float64()*2)-1)*p.jitterFraction
	}
	if delay < 0 {
		return 0
	}
	return time.Duration(delay)
}

// Run starts the server
func (s *Server) Run() error {
	if s.Port == 0 {
		return fmt.Errorf("server port must be set")
	}

	s.uuid = uuid.New().String()
	s.retryPolicy = loadRateRetryPolicy()
	go s.serveMetrics()

	opts := []grpc.ServerOption{
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Timeout: 120 * time.Second,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			PermitWithoutStream: true,
		}),
		grpc.UnaryInterceptor(
			otgrpc.OpenTracingServerInterceptor(s.Tracer),
		),
	}

	if tlsopt := tls.GetServerOpt(); tlsopt != nil {
		opts = append(opts, tlsopt)
	}

	srv := grpc.NewServer(opts...)
	pb.RegisterSearchServer(srv, s)

	// init grpc clients
	if err := s.initGeoClient("srv-geo"); err != nil {
		return err
	}
	if err := s.initRateClient("srv-rate"); err != nil {
		return err
	}

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", s.Port))
	if err != nil {
		log.Fatal().Msgf("failed to listen: %v", err)
	}

	// register with consul
	// jsonFile, err := os.Open("config.json")
	// if err != nil {
	// 	fmt.Println(err)
	// }

	// defer jsonFile.Close()

	// byteValue, _ := ioutil.ReadAll(jsonFile)

	// var result map[string]string
	// json.Unmarshal([]byte(byteValue), &result)

	err = s.Registry.Register(name, s.uuid, s.IpAddr, s.Port)
	if err != nil {
		return fmt.Errorf("failed register: %v", err)
	}
	log.Info().Msg("Successfully registered in consul")

	return srv.Serve(lis)
}

func (s *Server) serveMetrics() {
	port := envInt("SEARCH_METRICS_PORT", 9092)
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		fmt.Fprintf(w, "search_requests_total %d\n", atomic.LoadUint64(&s.metrics.requestsTotal))
		fmt.Fprintf(w, "search_rate_attempts_total %d\n", atomic.LoadUint64(&s.metrics.rateAttemptsTotal))
		fmt.Fprintf(w, "search_rate_timeouts_total %d\n", atomic.LoadUint64(&s.metrics.rateTimeoutsTotal))
		fmt.Fprintf(w, "search_rate_failures_total %d\n", atomic.LoadUint64(&s.metrics.rateFailuresTotal))
		fmt.Fprintf(w, "search_rate_successes_total %d\n", atomic.LoadUint64(&s.metrics.rateSuccessesTotal))
	})
	if err := http.ListenAndServe(fmt.Sprintf(":%d", port), mux); err != nil {
		log.Error().Err(err).Msg("search metrics server stopped")
	}
}

// Shutdown cleans up any processes
func (s *Server) Shutdown() {
	s.Registry.Deregister(s.uuid)
}

func (s *Server) initGeoClient(name string) error {
	conn, err := s.getGprcConn(name)
	if err != nil {
		return fmt.Errorf("dialer error: %v", err)
	}
	s.geoClient = geo.NewGeoClient(conn)
	return nil
}

func (s *Server) initRateClient(name string) error {
	conn, err := s.getGprcConn(name)
	if err != nil {
		return fmt.Errorf("dialer error: %v", err)
	}
	s.rateClient = rate.NewRateClient(conn)
	return nil
}

func (s *Server) getGprcConn(name string) (*grpc.ClientConn, error) {
	if s.KnativeDns != "" {
		return dialer.Dial(
			fmt.Sprintf("%s.%s", name, s.KnativeDns),
			dialer.WithTracer(s.Tracer))
	} else {
		return dialer.Dial(
			name,
			dialer.WithTracer(s.Tracer),
			dialer.WithBalancer(s.Registry.Client),
		)
	}
}

// Nearby returns ids of nearby hotels ordered by ranking algo
func (s *Server) Nearby(ctx context.Context, req *pb.NearbyRequest) (*pb.SearchResult, error) {
	atomic.AddUint64(&s.metrics.requestsTotal, 1)
	// find nearby hotels
	log.Trace().Msg("in Search Nearby")

	log.Trace().Msgf("nearby lat = %f", req.Lat)
	log.Trace().Msgf("nearby lon = %f", req.Lon)

	nearby, err := s.geoClient.Nearby(ctx, &geo.Request{
		Lat: req.Lat,
		Lon: req.Lon,
	})
	if err != nil {
		return nil, err
	}

	for _, hid := range nearby.HotelIds {
		log.Trace().Msgf("get Nearby hotelId = %s", hid)
	}

	// Find rates for hotels. Each attempt has its own deadline so a transient
	// downstream slowdown cannot occupy this service indefinitely.
	rates, err := s.getRates(ctx, &rate.Request{
		HotelIds: nearby.HotelIds,
		InDate:   req.InDate,
		OutDate:  req.OutDate,
	})
	if err != nil {
		log.Warn().Str("dependency", "rate").Str("grpc_code", grpc.Code(err).String()).Msg("downstream request failed")
		return nil, status.Error(codes.Unavailable, "search dependency unavailable")
	}

	// TODO(hw): add simple ranking algo to order hotel ids:
	// * geo distance
	// * price (best discount?)
	// * reviews

	// build the response
	res := new(pb.SearchResult)
	for _, ratePlan := range rates.RatePlans {
		log.Trace().Msgf("get RatePlan HotelId = %s, Code = %s", ratePlan.HotelId, ratePlan.Code)
		res.HotelIds = append(res.HotelIds, ratePlan.HotelId)
	}
	return res, nil
}

func (s *Server) getRates(ctx context.Context, req *rate.Request) (*rate.Result, error) {
	policy := s.retryPolicy
	var lastErr error

	for attempt := 1; attempt <= policy.maxAttempts; attempt++ {
		attemptCtx, cancel := context.WithTimeout(ctx, policy.timeout)
		atomic.AddUint64(&s.metrics.rateAttemptsTotal, 1)
		result, err := s.rateClient.GetRates(attemptCtx, req)
		deadlineExceeded := attemptCtx.Err() == context.DeadlineExceeded || grpc.Code(err) == codes.DeadlineExceeded
		cancel()

		if err == nil {
			atomic.AddUint64(&s.metrics.rateSuccessesTotal, 1)
			return result, nil
		}

		lastErr = err
		if deadlineExceeded {
			atomic.AddUint64(&s.metrics.rateTimeoutsTotal, 1)
		}
		code := grpc.Code(err)
		if ctx.Err() != nil ||
			(code != codes.DeadlineExceeded && code != codes.Unavailable && code != codes.ResourceExhausted) {
			break
		}
		if attempt < policy.maxAttempts {
			time.Sleep(policy.backoff(attempt))
		}
	}

	atomic.AddUint64(&s.metrics.rateFailuresTotal, 1)
	return nil, lastErr
}
