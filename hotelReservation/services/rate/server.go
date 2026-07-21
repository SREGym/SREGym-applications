package rate

import (
	"encoding/json"
	"fmt"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	// "io/ioutil"
	"net"
	"net/http"
	"os"
	// "os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/google/uuid"
	"github.com/grpc-ecosystem/grpc-opentracing/go/otgrpc"
	"github.com/harlow/go-micro-services/registry"
	pb "github.com/harlow/go-micro-services/services/rate/proto"
	"github.com/harlow/go-micro-services/tls"
	"github.com/opentracing/opentracing-go"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"

	"strings"

	"github.com/bradfitz/gomemcache/memcache"
)

const name = "srv-rate"

type requestPacer struct {
	tokens        chan struct{}
	queueSlots    chan struct{}
	queueDepth    int64
	inFlight      int64
	received      uint64
	completed     uint64
	expired       uint64
	rejected      uint64
	qpsLimit      int
	queueCapacity int
}

// Server implements the rate service
type Server struct {
	Tracer       opentracing.Tracer
	Port         int
	IpAddr       string
	MongoSession *mgo.Session
	Registry     *registry.Client
	MemcClient   *memcache.Client
	uuid         string
	pacer        *requestPacer
}

func envPositiveInt(name string, fallback int) int {
	value, err := strconv.Atoi(os.Getenv(name))
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}

func newRequestPacer(qpsLimit, queueCapacity int) *requestPacer {
	p := &requestPacer{
		tokens:        make(chan struct{}, 1),
		queueSlots:    make(chan struct{}, queueCapacity),
		qpsLimit:      qpsLimit,
		queueCapacity: queueCapacity,
	}
	p.tokens <- struct{}{}
	interval := time.Second / time.Duration(qpsLimit)
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for range ticker.C {
			select {
			case p.tokens <- struct{}{}:
			default:
			}
		}
	}()
	return p
}

func (p *requestPacer) waitForCapacity() error {
	select {
	case p.queueSlots <- struct{}{}:
		atomic.AddInt64(&p.queueDepth, 1)
	default:
		atomic.AddUint64(&p.rejected, 1)
		return fmt.Errorf("rate request queue is full")
	}

	<-p.tokens
	<-p.queueSlots
	atomic.AddInt64(&p.queueDepth, -1)
	atomic.AddInt64(&p.inFlight, 1)
	return nil
}

// Run starts the server
func (s *Server) Run() error {
	opentracing.SetGlobalTracer(s.Tracer)

	if s.Port == 0 {
		return fmt.Errorf("server port must be set")
	}

	s.uuid = uuid.New().String()
	qpsLimit := envPositiveInt("RATE_BACKEND_QPS_LIMIT", 20)
	s.pacer = newRequestPacer(qpsLimit, envPositiveInt("RATE_QUEUE_CAPACITY", 256))
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

	pb.RegisterRateServer(srv, s)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", s.Port))
	if err != nil {
		log.Fatal().Msgf("failed to listen: %v", err)
	}

	// register the service
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
	port := envPositiveInt("RATE_METRICS_PORT", 9091)
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		fmt.Fprintf(w, "rate_backend_qps_limit %d\n", s.pacer.qpsLimit)
		fmt.Fprintf(w, "rate_queue_capacity %d\n", s.pacer.queueCapacity)
		fmt.Fprintf(w, "rate_queue_depth %d\n", atomic.LoadInt64(&s.pacer.queueDepth))
		fmt.Fprintf(w, "rate_in_flight %d\n", atomic.LoadInt64(&s.pacer.inFlight))
		fmt.Fprintf(w, "rate_requests_received_total %d\n", atomic.LoadUint64(&s.pacer.received))
		fmt.Fprintf(w, "rate_requests_completed_total %d\n", atomic.LoadUint64(&s.pacer.completed))
		fmt.Fprintf(w, "rate_requests_expired_total %d\n", atomic.LoadUint64(&s.pacer.expired))
		fmt.Fprintf(w, "rate_requests_rejected_total %d\n", atomic.LoadUint64(&s.pacer.rejected))
	})
	if err := http.ListenAndServe(fmt.Sprintf(":%d", port), mux); err != nil {
		log.Error().Err(err).Msg("rate metrics server stopped")
	}
}

// Shutdown cleans up any processes
func (s *Server) Shutdown() {
	s.Registry.Deregister(s.uuid)
}

// GetRates gets rates for hotels for specific date range.
func (s *Server) GetRates(ctx context.Context, req *pb.Request) (*pb.Result, error) {
	if s.pacer != nil {
		atomic.AddUint64(&s.pacer.received, 1)
		if err := s.pacer.waitForCapacity(); err != nil {
			return nil, status.Error(codes.ResourceExhausted, err.Error())
		}
		defer func() {
			atomic.AddInt64(&s.pacer.inFlight, -1)
			atomic.AddUint64(&s.pacer.completed, 1)
			if ctx.Err() != nil {
				atomic.AddUint64(&s.pacer.expired, 1)
			}
		}()
	}
	res := new(pb.Result)
	// session, err := mgo.Dial("mongodb-rate")
	// if err != nil {
	// 	panic(err)
	// }
	// defer session.Close()

	ratePlans := make(RatePlans, 0)

	hotelIds := []string{}
	rateMap := make(map[string]struct{})
	for _, hotelID := range req.HotelIds {
		hotelIds = append(hotelIds, hotelID)
		rateMap[hotelID] = struct{}{}
	}
	// first check memcached(get-multi)
	memSpan, _ := opentracing.StartSpanFromContext(ctx, "memcached_get_multi_rate")
	memSpan.SetTag("span.kind", "client")
	resMap, err := s.MemcClient.GetMulti(hotelIds)
	memSpan.Finish()
	var wg sync.WaitGroup
	var mutex sync.Mutex
	if err != nil && err != memcache.ErrCacheMiss {
		log.Panic().Msgf("Memmcached error while trying to get hotel [id: %v]= %s", hotelIds, err)
	} else {
		for hotelId, item := range resMap {
			rateStrs := strings.Split(string(item.Value), "\n")
			log.Trace().Msgf("memc hit, hotelId = %s,rate strings: %v", hotelId, rateStrs)

			for _, rateStr := range rateStrs {
				if len(rateStr) != 0 {
					rateP := new(pb.RatePlan)
					json.Unmarshal([]byte(rateStr), rateP)
					ratePlans = append(ratePlans, rateP)
				}
			}
			delete(rateMap, hotelId)
		}
		wg.Add(len(rateMap))
		for hotelId := range rateMap {
			go func(id string) {
				log.Trace().Msgf("memc miss, hotelId = %s", id)
				log.Trace().Msg("memcached miss, set up mongo connection")

				// memcached miss, set up mongo connection
				session := s.MongoSession.Copy()
				defer session.Close()
				c := session.DB("rate-db").C("inventory")
				memcStr := ""
				tmpRatePlans := make(RatePlans, 0)
				mongoSpan, _ := opentracing.StartSpanFromContext(ctx, "mongo_rate")
				mongoSpan.SetTag("span.kind", "client")
				err := c.Find(&bson.M{"hotelId": id}).All(&tmpRatePlans)
				mongoSpan.Finish()
				if err != nil {
					log.Panic().Msgf("Tried to find hotelId [%v], but got error", id, err.Error())
				} else {
					for _, r := range tmpRatePlans {
						mutex.Lock()
						ratePlans = append(ratePlans, r)
						mutex.Unlock()
						rateJson, err := json.Marshal(r)
						if err != nil {
							log.Error().Msgf("Failed to marshal plan [Code: %v] with error: %s", r.Code, err)
						}
						memcStr = memcStr + string(rateJson) + "\n"
					}
				}
				go s.MemcClient.Set(&memcache.Item{Key: id, Value: []byte(memcStr)})

				defer wg.Done()
			}(hotelId)
		}
	}
	wg.Wait()

	sort.Sort(ratePlans)
	res.RatePlans = ratePlans

	return res, nil
}

type RatePlans []*pb.RatePlan

func (r RatePlans) Len() int {
	return len(r)
}

func (r RatePlans) Swap(i, j int) {
	r[i], r[j] = r[j], r[i]
}

func (r RatePlans) Less(i, j int) bool {
	return r[i].RoomType.TotalRate > r[j].RoomType.TotalRate
}
