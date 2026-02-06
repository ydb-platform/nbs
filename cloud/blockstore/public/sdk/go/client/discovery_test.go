package client

import (
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	protos "github.com/ydb-platform/nbs/cloud/blockstore/public/api/protos"
	"golang.org/x/net/context"
)

////////////////////////////////////////////////////////////////////////////////

type testStats struct {
	requestsCount int
	aliveClients  int
	slowRequests  int
}

type testClientImpl struct {
	testClient

	name string

	fire       chan struct{}
	hasRequest chan *testClientImpl
}

type testEnv struct {
	hasRequest chan *testClientImpl
	clients    map[string]*testClientImpl
}

func newStderrLog() Logger {
	return NewLog(log.New(os.Stderr, "", log.Lmicroseconds), LOG_DEBUG)
}

func newGoodClient(
	name string,
	hasRequest chan *testClientImpl,
	stats *testStats,
) *testClientImpl {

	c := &testClientImpl{
		testClient: testClient{
			CloseHandlerFunc: func() error {
				stats.aliveClients--
				return nil
			},
		},
		name:       name,
		hasRequest: hasRequest,
	}

	c.DescribeVolumeHandler = func(
		ctx context.Context,
		req *protos.TDescribeVolumeRequest,
	) (*protos.TDescribeVolumeResponse, error) {
		stats.requestsCount += 1
		c.hasRequest <- c
		return &protos.TDescribeVolumeResponse{}, nil
	}

	return c
}

func newSlowClient(
	name string,
	hasRequest chan *testClientImpl,
	stats *testStats,
) *testClientImpl {

	c := &testClientImpl{
		testClient: testClient{
			CloseHandlerFunc: func() error {
				stats.aliveClients--
				return nil
			},
		},
		name:       name,
		fire:       make(chan struct{}),
		hasRequest: hasRequest,
	}

	c.DescribeVolumeHandler = func(
		ctx context.Context,
		req *protos.TDescribeVolumeRequest,
	) (*protos.TDescribeVolumeResponse, error) {
		stats.requestsCount += 1
		c.hasRequest <- c

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-c.fire:
			return nil, &ClientError{
				Code: E_REJECTED,
			}
		}
	}

	return c
}

func newBadClient(
	name string,
	hasRequest chan *testClientImpl,
	stats *testStats,
) *testClientImpl {

	c := &testClientImpl{
		testClient: testClient{
			CloseHandlerFunc: func() error {
				stats.aliveClients--
				return nil
			},
		},
		name:       name,
		hasRequest: hasRequest,
	}

	c.DescribeVolumeHandler = func(
		ctx context.Context,
		req *protos.TDescribeVolumeRequest,
	) (*protos.TDescribeVolumeResponse, error) {
		stats.requestsCount += 1
		c.hasRequest <- c

		return nil, &ClientError{
			Code: E_REJECTED,
		}
	}

	return c
}

func describeVolume(ctx context.Context, client ClientIface) error {
	_, err := client.DescribeVolume(
		ctx,
		&protos.TDescribeVolumeRequest{},
	)
	return err
}

func createBalancer(hosts ...[]string) ClientIface {
	snapshots := [][]*instanceAddress{}

	for _, ls := range hosts {
		snapshot := []*instanceAddress{}
		for _, host := range ls {
			snapshot = append(snapshot, &instanceAddress{
				Host: host,
			})
		}

		snapshots = append(snapshots, snapshot)
	}

	idx := 0
	return &testClient{
		DiscoverInstancesHandler: func(
			ctx context.Context,
			req *protos.TDiscoverInstancesRequest,
		) (*protos.TDiscoverInstancesResponse, error) {
			resp := &protos.TDiscoverInstancesResponse{}

			if idx < len(snapshots) {
				resp.Instances = snapshots[idx]
				idx++
			}

			if int(req.Limit) < len(resp.Instances) {
				resp.Instances = resp.Instances[:req.Limit]
			}

			return resp, nil
		},
		PingHandler: func(
			ctx context.Context,
			req *protos.TPingRequest,
		) (*protos.TPingResponse, error) {
			return &protos.TPingResponse{}, nil
		},
	}
}

func createSlowBalancer(delay time.Duration, hosts ...string) ClientIface {
	instances := []*instanceAddress{}
	for _, host := range hosts {
		instances = append(instances, &instanceAddress{Host: host})
	}

	return &testClient{
		DiscoverInstancesHandler: func(
			ctx context.Context,
			req *protos.TDiscoverInstancesRequest,
		) (*protos.TDiscoverInstancesResponse, error) {
			resp := &protos.TDiscoverInstancesResponse{}
			resp.Instances = instances

			if int(req.Limit) < len(resp.Instances) {
				resp.Instances = resp.Instances[:req.Limit]
			}

			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(delay):
				return resp, nil
			}
		},
		PingHandler: func(
			ctx context.Context,
			req *protos.TPingRequest,
		) (*protos.TPingResponse, error) {

			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(delay):
				return &protos.TPingResponse{}, nil
			}
		},
	}
}

func testDiscoveryClientLimit(t *testing.T, failExpected bool) {
	balancer := createSlowBalancer(100*time.Millisecond, "bad-1", "bad-2", "good")

	opts := &DiscoveryClientOpts{}

	if failExpected {
		opts.HardTimeout = 1 * time.Second
		opts.Limit = 2
	}

	stats := testStats{}

	factory := func(host string, _ uint32) (ClientIface, error) {
		stats.aliveClients++
		if strings.HasPrefix(host, "good") {
			return &testClient{
				CloseHandlerFunc: func() error {
					stats.aliveClients--
					return nil
				},
				DescribeVolumeHandler: func(
					ctx context.Context,
					req *protos.TDescribeVolumeRequest,
				) (*protos.TDescribeVolumeResponse, error) {
					stats.requestsCount++
					return &protos.TDescribeVolumeResponse{}, nil
				},
			}, nil
		}

		return &testClient{
			CloseHandlerFunc: func() error {
				stats.aliveClients--
				return nil
			},
			DescribeVolumeHandler: func(
				ctx context.Context,
				req *protos.TDescribeVolumeRequest,
			) (*protos.TDescribeVolumeResponse, error) {
				stats.requestsCount++
				return nil, &ClientError{
					Code: E_REJECTED,
				}
			},
		}, nil
	}

	logger := newStderrLog()

	discovery := newDiscoveryClient([]ClientIface{balancer}, opts, factory, logger, false)

	err := describeVolume(context.TODO(), discovery)

	if !failExpected && err != nil {
		t.Error(err)
	}

	if failExpected && err == nil {
		t.Errorf("Unexpectedly missing error")
	}

	_ = discovery.Close()
	if stats.aliveClients != 0 {
		t.Fatalf("client leak detected: %d", stats.aliveClients)
	}
}

type planFn = func(env testEnv) error
type testPlan = []planFn

func request_to(host string) planFn {
	return func(env testEnv) error {
		c := <-env.hasRequest

		if c.name != host {
			return fmt.Errorf("unexpected request: %v, expected: %v", c.name, host)
		}

		return nil
	}
}

func response_from(host string) planFn {
	return func(env testEnv) error {
		c := env.clients[host]
		c.fire <- struct{}{}
		close(c.fire)

		return nil
	}
}

func fire_deadline() planFn {
	return nil
}

func testDiscoveryClientSoftTimeoutSimple(
	t *testing.T,
	instances [][]string,
	expectedRequestsCount int, plan testPlan,
	failExpected bool,
) {
	balancer := createBalancer(instances...)

	ctx, cancelCtx := context.WithCancel(context.Background())
	defer cancelCtx()

	opts := &DiscoveryClientOpts{
		HardTimeout: 100 * time.Second,
		SoftTimeout: 100 * time.Millisecond,
		Limit:       uint32(len(instances[0])),
	}

	stats := testStats{}

	env := testEnv{
		hasRequest: make(chan *testClientImpl),
		clients:    map[string]*testClientImpl{},
	}

	for _, host := range instances[0] {
		if host == "broken" {
			continue
		}

		if host == "good" {
			env.clients[host] = newGoodClient(host, env.hasRequest, &stats)
			continue
		}

		if strings.HasPrefix(host, "bad") {
			env.clients[host] = newBadClient(host, env.hasRequest, &stats)
			continue
		}

		env.clients[host] = newSlowClient(host, env.hasRequest, &stats)
	}

	factory := func(host string, _ uint32) (ClientIface, error) {
		if host == "broken" {
			return nil, errors.New("broken client")
		}

		stats.aliveClients++

		return env.clients[host], nil
	}

	logger := newStderrLog()
	discovery := newDiscoveryClient([]ClientIface{balancer}, opts, factory, logger, false)

	done := make(chan error)

	go func() {
		done <- describeVolume(ctx, discovery)
		close(done)
	}()

	for _, fn := range plan {
		if fn == nil {
			cancelCtx()
			break
		}
		err := fn(env)
		if err != nil {
			t.Error(err)
		}
	}

	err := <-done

	if !failExpected && err != nil {
		t.Error(err)
	}

	if failExpected && err == nil {
		t.Errorf("Unexpectedly missing error")
	}

	if stats.requestsCount != expectedRequestsCount {
		t.Fatalf(
			"Unexpected requests count: %d. Expected %d",
			stats.requestsCount,
			expectedRequestsCount,
		)
	}

	_ = discovery.Close()
	if stats.aliveClients != 0 {
		t.Fatalf("client leak detected: %d", stats.aliveClients)
	}
}

func testDiscoveryClientDiscover(
	t *testing.T,
	failExpected bool,
) {
	stats := testStats{}

	factory := func(host string, _ uint32) (ClientIface, error) {
		stats.aliveClients++

		if host == "good" {
			return &testClient{
				CloseHandlerFunc: func() error {
					stats.aliveClients--
					return nil
				},
				PingHandler: func(
					ctx context.Context,
					req *protos.TPingRequest,
				) (*protos.TPingResponse, error) {
					stats.requestsCount++
					return &protos.TPingResponse{}, nil
				},
				DescribeVolumeHandler: func(
					ctx context.Context,
					req *protos.TDescribeVolumeRequest,
				) (*protos.TDescribeVolumeResponse, error) {
					stats.requestsCount++
					return &protos.TDescribeVolumeResponse{
						Volume: &protos.TVolume{},
					}, nil
				},
			}, nil
		}

		return &testClient{
			CloseHandlerFunc: func() error {
				stats.aliveClients--
				return nil
			},
			PingHandler: func(
				ctx context.Context,
				req *protos.TPingRequest,
			) (*protos.TPingResponse, error) {
				stats.requestsCount++
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(100 * time.Millisecond):
					return nil, &ClientError{
						Code: E_REJECTED,
					}
				}
			},
		}, nil
	}

	balancer := createSlowBalancer(1*time.Millisecond, "bad-1", "bad-2", "good")

	opts := &DiscoveryClientOpts{}

	if failExpected {
		opts.HardTimeout = 1 * time.Second
		opts.Limit = 2
	}

	logger := newStderrLog()
	impl := newDiscoveryClient([]ClientIface{balancer}, opts, factory, logger, false)

	discovery := &DiscoveryClient{
		safeClient{impl},
	}

	client, host, err := discovery.DiscoverInstance(context.TODO())

	if !failExpected && err != nil {
		t.Error(err)
	}

	if failExpected && err == nil {
		t.Errorf("Unexpectedly missing error")
	}

	if !failExpected && host != "good" {
		t.Errorf("Unexpected host %v", host)
	}

	if !failExpected {
		_, err = client.DescribeVolume(
			context.TODO(),
			"disk-id",
		)

		if err != nil {
			t.Error(err)
		}

		_ = client.Close()
	}

	_ = discovery.Close()

	if stats.aliveClients != 0 {
		t.Fatalf("client leak detected: %d", stats.aliveClients)
	}
}

////////////////////////////////////////////////////////////////////////////////

func TestDiscoveryClientLimitFail(t *testing.T) {
	testDiscoveryClientLimit(t, true)
}

func TestDiscoveryClientLimitSuccess(t *testing.T) {
	testDiscoveryClientLimit(t, false)
}

func TestDiscoveryClientNoRetriableError(t *testing.T) {
	balancer := createBalancer([]string{"bad", "good"})

	aliveClients := 0

	factory := func(host string, _ uint32) (ClientIface, error) {
		aliveClients++
		if host == "good" {
			return &testClient{
				CloseHandlerFunc: func() error {
					aliveClients--
					return nil
				},
			}, nil
		}

		return &testClient{
			CloseHandlerFunc: func() error {
				aliveClients--
				return nil
			},
			DescribeVolumeHandler: func(
				ctx context.Context,
				req *protos.TDescribeVolumeRequest,
			) (*protos.TDescribeVolumeResponse, error) {
				return nil, &ClientError{
					Code: E_INVALID_STATE,
				}
			},
		}, nil
	}

	opts := &DiscoveryClientOpts{}
	logger := newStderrLog()
	discovery := newDiscoveryClient([]ClientIface{balancer}, opts, factory, logger, false)

	err := describeVolume(context.TODO(), discovery)

	if err == nil {
		t.Errorf("Unexpectedly missing error")
	}

	if GetClientError(err).Code != E_INVALID_STATE {
		t.Errorf("Unexpected error code %v", err)
	}

	_ = discovery.Close()
	if aliveClients != 0 {
		t.Fatalf("client leak detected: %d", aliveClients)
	}
}

func TestDiscoveryClientVolatileNetwork(t *testing.T) {
	alive := map[string]bool{
		"host-1": false,
		"host-2": false,
		"host-3": true,
		"host-4": true,
	}

	balancer := createBalancer(
		[]string{
			"host-1",
			"host-2",
			"host-3",
		},
		[]string{
			"host-1",
			"host-3",
			"host-4",
		},
	)

	route := []string{}

	aliveClients := 0

	factory := func(host string, _ uint32) (ClientIface, error) {
		aliveClients++
		return &testClient{
			CloseHandlerFunc: func() error {
				aliveClients--
				return nil
			},
			DescribeVolumeHandler: func(
				ctx context.Context,
				req *protos.TDescribeVolumeRequest,
			) (*protos.TDescribeVolumeResponse, error) {
				route = append(route, host)

				if alive[host] {
					return &protos.TDescribeVolumeResponse{}, nil
				}

				return nil, &ClientError{
					Code: E_REJECTED,
				}
			},
		}, nil
	}

	opts := &DiscoveryClientOpts{}
	logger := newStderrLog()
	discovery := newDiscoveryClient([]ClientIface{balancer}, opts, factory, logger, false)

	shoot := func() {
		err := describeVolume(context.TODO(), discovery)
		if err != nil {
			t.Error(err)
		}
	}

	shoot()

	alive["host-3"] = false

	shoot()

	expectedRoute := []string{
		"host-1", "host-2", "host-3", "host-1", "host-3", "host-4",
	}

	if !reflect.DeepEqual(route, expectedRoute) {
		t.Fatalf(
			"Unexpected route %v. Expected %v",
			route,
			expectedRoute,
		)
	}

	_ = discovery.Close()
	if aliveClients != 0 {
		t.Fatalf("client leak detected: %d", aliveClients)
	}
}

func TestDiscoveryClientEmptyInstances(t *testing.T) {
	balancer := createSlowBalancer(100 * time.Millisecond)

	aliveClients := 0
	opts := &DiscoveryClientOpts{
		HardTimeout: 1 * time.Second,
	}
	factory := func(_ string, _ uint32) (ClientIface, error) {
		aliveClients++
		return &testClient{
			CloseHandlerFunc: func() error {
				aliveClients--
				return nil
			},
		}, nil
	}

	logger := newStderrLog()
	discovery := newDiscoveryClient([]ClientIface{balancer}, opts, factory, logger, false)

	err := describeVolume(context.TODO(), discovery)
	if err == nil {
		t.Errorf("Unexpectedly missing error")
	}

	_ = discovery.Close()
	if aliveClients != 0 {
		t.Fatalf("client leak detected: %d", aliveClients)
	}
}

func TestDiscoveryClientGoodOne(t *testing.T) {
	balancer := &testClient{
		DiscoverInstancesHandler: func(
			ctx context.Context,
			req *protos.TDiscoverInstancesRequest,
		) (*protos.TDiscoverInstancesResponse, error) {
			resp := &protos.TDiscoverInstancesResponse{}

			resp.Instances = []*instanceAddress{
				&instanceAddress{
					Host: "good",
				}}

			return resp, nil
		},
		PingHandler: func(
			ctx context.Context,
			req *protos.TPingRequest,
		) (*protos.TPingResponse, error) {
			return &protos.TPingResponse{}, nil
		},
	}

	requestsCount := 0
	aliveClients := 0
	opts := &DiscoveryClientOpts{}
	factory := func(_ string, _ uint32) (ClientIface, error) {
		aliveClients++
		return &testClient{
			CloseHandlerFunc: func() error {
				aliveClients--
				return nil
			},
			DescribeVolumeHandler: func(
				ctx context.Context,
				req *protos.TDescribeVolumeRequest,
			) (*protos.TDescribeVolumeResponse, error) {
				requestsCount++
				return &protos.TDescribeVolumeResponse{}, nil
			},
		}, nil
	}

	logger := newStderrLog()
	discovery := newDiscoveryClient([]ClientIface{balancer}, opts, factory, logger, false)

	describeRequests := 10

	for i := 0; i != describeRequests; i++ {
		err := describeVolume(context.TODO(), discovery)
		if err != nil {
			t.Error(err)
		}
	}

	if requestsCount != describeRequests {
		t.Fatalf(
			"Unexpected requests count: %d. Expected %d",
			requestsCount,
			describeRequests,
		)
	}

	_ = discovery.Close()
	if aliveClients != 0 {
		t.Fatalf("client leak detected: %d", aliveClients)
	}
}

func TestDiscoveryClientHardTimeout(t *testing.T) {
	balancer := createBalancer([]string{
		"slow-1",
		"slow-2",
		"good",
	})

	opts := &DiscoveryClientOpts{
		HardTimeout: 1500 * time.Millisecond,
	}

	requestsCount := 0
	aliveClients := 0

	factory := func(host string, _ uint32) (ClientIface, error) {
		aliveClients++
		if host == "good" {
			return &testClient{
				CloseHandlerFunc: func() error {
					aliveClients--
					return nil
				},
				DescribeVolumeHandler: func(
					ctx context.Context,
					req *protos.TDescribeVolumeRequest,
				) (*protos.TDescribeVolumeResponse, error) {
					requestsCount++
					return &protos.TDescribeVolumeResponse{}, nil
				},
			}, nil
		}
		return &testClient{
			CloseHandlerFunc: func() error {
				aliveClients--
				return nil
			},
			DescribeVolumeHandler: func(
				ctx context.Context,
				req *protos.TDescribeVolumeRequest,
			) (*protos.TDescribeVolumeResponse, error) {
				requestsCount++
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(900 * time.Millisecond):
					return nil, &ClientError{
						Code: E_REJECTED,
					}
				}
			},
		}, nil
	}

	logger := newStderrLog()
	discovery := newDiscoveryClient([]ClientIface{balancer}, opts, factory, logger, false)

	err := describeVolume(context.TODO(), discovery)

	if err == nil {
		t.Errorf("Unexpectedly missing error")
	}

	var cerr *ClientError
	if !errors.As(err, &cerr) {
		t.Errorf("Unexpected error type: %v", err)
	}

	if cerr == nil || cerr.Code != E_TIMEOUT {
		t.Errorf("Unexpected error code: %v", cerr)
	}

	expectedRequestsCount := 2 // slow-1 & slow-2

	if requestsCount != expectedRequestsCount {
		t.Fatalf(
			"Unexpected requests count: %d. Expected %d",
			requestsCount,
			expectedRequestsCount,
		)
	}

	_ = discovery.Close()
	if aliveClients != 0 {
		t.Fatalf("client leak detected: %d", aliveClients)
	}
}

func TestDiscoveryClientSoftTimeout1(t *testing.T) {
	testDiscoveryClientSoftTimeoutSimple(t,
		[][]string{
			[]string{
				"ugly",
				"good",
			},
			[]string{
				"good",
				"ugly",
			},
		},
		2,
		testPlan{
			request_to("ugly"),
			request_to("good"),
		},
		false,
	)
}

func TestDiscoveryClientSoftTimeout2(t *testing.T) {
	testDiscoveryClientSoftTimeoutSimple(
		t,
		[][]string{
			[]string{
				"bad",
				"ugly",
				"good",
			},
			[]string{
				"good",
				"ugly",
				"bad",
			},
		},
		3,
		testPlan{
			request_to("bad"),
			request_to("ugly"),
			request_to("good"),
		},
		false,
	)
}

func TestDiscoveryClientSoftTimeout3(t *testing.T) {
	testDiscoveryClientSoftTimeoutSimple(
		t,
		[][]string{
			[]string{
				"ugly-1",
				"good",
				"ugly-2",
			},
			[]string{
				"ugly-2",
				"good",
				"ugly-1",
			},
		},
		3,
		testPlan{
			request_to("ugly-1"),
			request_to("ugly-2"),
			response_from("ugly-1"),
			response_from("ugly-2"),
			request_to("good"),
		},
		false,
	)
}

func TestDiscoveryClientSoftTimeout4(t *testing.T) {
	testDiscoveryClientSoftTimeoutSimple(
		t,
		[][]string{
			[]string{
				"ugly",
				"bad",
				"good",
			},
			[]string{
				"bad",
				"good",
				"ugly",
			},
		},
		2,
		testPlan{
			request_to("ugly"),
			request_to("bad"),
			fire_deadline(),
		},
		true, // a good one can't help
	)
}

func TestDiscoveryClientSoftTimeout5(t *testing.T) {
	testDiscoveryClientSoftTimeoutSimple(t,
		[][]string{
			[]string{
				"ugly",
				"broken",
				"good",
			},
			[]string{
				"good",
				"ugly",
				"broken",
			},
		},
		2,
		testPlan{
			request_to("ugly"),
			request_to("good"),
		},
		false,
	)
}

func TestDiscoveryClientDiscoverFail(t *testing.T) {
	testDiscoveryClientDiscover(t, true)
}

func TestDiscoveryClientDiscoverSuccess(t *testing.T) {
	testDiscoveryClientDiscover(t, false)
}

func testFindClosest(t *testing.T, endpoints []string, expected string, timeout time.Duration) {
	clients := make([]ClientIface, 0)
	names := make(map[ClientIface]string)

	addClient := func(
		name string,
		delay time.Duration,
		res *protos.TPingResponse,
		err error,
	) {
		ping := func(
			ctx context.Context,
			req *protos.TPingRequest,
		) (*protos.TPingResponse, error) {
			time.Sleep(delay * time.Millisecond) // TODO mock timer
			return res, err
		}
		client := &testClient{PingHandler: ping}
		clients = append(clients, client)
		names[client] = name
	}

	for _, endpoint := range endpoints {
		var host string
		var delay int

		_, err := fmt.Sscanf(endpoint, "%s %d", &host, &delay)

		if err != nil {
			continue
		}

		if host == "good" {
			addClient(endpoint, time.Duration(delay), &protos.TPingResponse{}, nil)
		}

		if host == "bad" {
			addClient(endpoint, time.Duration(delay), nil, &ClientError{Code: E_FAIL})
		}
	}

	ctx, cancel := context.WithTimeout(context.TODO(), timeout*time.Millisecond)
	defer cancel()

	val_chan := make(chan ClientIface)
	err_chan := make(chan error)

	go func() {
		val, err := findClosest(ctx, clients)
		val_chan <- val
		err_chan <- err
	}()

	val := names[<-val_chan]
	err := <-err_chan

	if val != expected {
		t.Errorf("Expected: %v, got: %v", expected, val)
	}

	if val == "" && err == nil {
		t.Errorf("Error must be set")
	}

	if val != "" && err != nil {
		t.Errorf("Error must be %v", nil)
	}
}

func TestFindClosestSuccess(t *testing.T) {
	testFindClosest(t, []string{"unknown", "bad 100", "good 100", "good 200"}, "good 100", 250)
}

func TestFindClosestTimeoutError(t *testing.T) {
	testFindClosest(t, []string{"bad 100", "good 200"}, "", 150)
}

func TestFindClosestPingError(t *testing.T) {
	testFindClosest(t, []string{"bad 100", "bad 200"}, "", 250)
}

func TestFindClosestClientCreationError(t *testing.T) {
	testFindClosest(t, []string{"unknown"}, "", 250)
}

func TestFindClosestNoEndpointsError(t *testing.T) {
	testFindClosest(t, []string{}, "", 250)
}

func TestDiscoveryClientSlowBalancers(t *testing.T) {
	b1 := createSlowBalancer(100*time.Second, "bad-1", "bad-2", "bad-3")
	b2 := createBalancer(
		[]string{
			"bad-1", "bad-2", "good",
		},
		[]string{
			"good", "bad-1", "bad-2",
		},
	)
	b3 := createSlowBalancer(1*time.Millisecond, "bad-1", "bad-2", "bad-3")

	opts := &DiscoveryClientOpts{
		SoftTimeout: 1 * time.Second,
	}

	requests := 0

	factory := func(host string, _ uint32) (ClientIface, error) {
		if strings.HasPrefix(host, "good") {
			return &testClient{
				DescribeVolumeHandler: func(
					ctx context.Context,
					req *protos.TDescribeVolumeRequest,
				) (*protos.TDescribeVolumeResponse, error) {
					requests++
					return &protos.TDescribeVolumeResponse{}, nil
				},
			}, nil
		}

		return &testClient{
			DescribeVolumeHandler: func(
				ctx context.Context,
				req *protos.TDescribeVolumeRequest,
			) (*protos.TDescribeVolumeResponse, error) {
				requests++
				return nil, &ClientError{
					Code: E_REJECTED,
				}
			},
		}, nil
	}

	rand.Seed(1)

	logger := newStderrLog()
	discovery := newDiscoveryClient([]ClientIface{b1, b2, b3}, opts, factory, logger, false)

	err := describeVolume(context.TODO(), discovery)

	if err != nil {
		t.Error(err)
	}

	// bad-1, bad-2, good
	if requests != 3 {
		t.Errorf("Unexpectedly requests %v (expected: 3)", requests)
	}

	err = describeVolume(context.TODO(), discovery)

	if err != nil {
		t.Error(err)
	}

	// good
	if requests != 4 {
		t.Errorf("Unexpectedly requests %v (expected: 4)", requests)
	}
}

func TestDiscoveryClientCachedBalancers(t *testing.T) {

	type balancerState struct {
		pings       int
		maxPings    int
		requests    int
		maxRequests int
		delay       time.Duration
	}

	createBalancer := func(state *balancerState) ClientIface {
		return &testClient{
			DiscoverInstancesHandler: func(
				ctx context.Context,
				req *protos.TDiscoverInstancesRequest,
			) (*protos.TDiscoverInstancesResponse, error) {
				state.requests++

				if state.maxRequests > 0 && state.requests >= state.maxRequests {
					return nil, &ClientError{
						Code:    E_REJECTED,
						Message: "too many requests",
					}
				}

				resp := &protos.TDiscoverInstancesResponse{
					Instances: []*instanceAddress{
						&instanceAddress{
							Host: "good",
						},
					},
				}
				return resp, nil
			},
			PingHandler: func(
				ctx context.Context,
				req *protos.TPingRequest,
			) (*protos.TPingResponse, error) {
				state.pings++

				if state.delay != 0 {
					<-time.After(state.delay)
				}

				if state.maxPings > 0 && state.pings >= state.maxPings {
					return nil, &ClientError{
						Code:    E_REJECTED,
						Message: "too many pings",
					}
				}
				return &protos.TPingResponse{}, nil
			},
		}
	}

	fooState := balancerState{
		maxPings:    2,
		maxRequests: 2,
	}
	barState := balancerState{
		delay: 10 * time.Millisecond,
	}

	foo := createBalancer(&fooState)
	bar := createBalancer(&barState)

	opts := &DiscoveryClientOpts{}
	logger := newStderrLog()

	discovery := newDiscoveryClient(
		[]ClientIface{
			foo,
			bar,
		},
		opts,
		func(_ string, _ uint32) (ClientIface, error) {
			return &testClient{
				CloseHandlerFunc: func() error {
					return nil
				},
				DescribeVolumeHandler: func(
					ctx context.Context,
					req *protos.TDescribeVolumeRequest,
				) (*protos.TDescribeVolumeResponse, error) {
					return &protos.TDescribeVolumeResponse{}, nil
				},
			}, nil
		},
		logger,
		false,
	)

	// find closest -> foo
	err := describeVolume(context.TODO(), discovery)
	if err != nil {
		t.Error(err)
	}

	if fooState.pings != 1 {
		t.Errorf("Unexpectedly pings for `foo`: %v (expected: 1)", fooState.pings)
	}

	if barState.pings != 1 {
		t.Errorf("Unexpectedly pings for `bar`: %v (expected: 1)", barState.pings)
	}

	if fooState.requests != 1 {
		t.Errorf("Unexpectedly requests for `foo`: %v (expected: 1)", fooState.requests)
	}

	if barState.requests != 0 {
		t.Errorf("Unexpectedly requests for `bar`: %v (expected: 0)", barState.requests)
	}

	// uses cashed balancer `foo` & gets error
	// then find closest -> bar & sends discovery request to `bar`
	err = describeVolume(context.TODO(), discovery)
	if err != nil {
		t.Error(err)
	}

	if fooState.pings != 2 {
		t.Errorf("Unexpectedly pings for `foo`: %v (expected: 2)", fooState.pings)
	}

	if barState.pings != 2 {
		t.Errorf("Unexpectedly pings for `bar`: %v (expected: 2)", barState.pings)
	}

	if fooState.requests != 2 {
		t.Errorf("Unexpectedly requests for `foo`: %v (expected: 2)", fooState.requests)
	}

	if barState.requests != 1 {
		t.Errorf("Unexpectedly requests for `bar`: %v (expected: 1)", barState.requests)
	}

	// uses `bar`
	err = describeVolume(context.TODO(), discovery)
	if err != nil {
		t.Error(err)
	}

	if fooState.pings != 2 {
		t.Errorf("Unexpectedly pings for `foo`: %v (expected: 2)", fooState.pings)
	}

	if barState.pings != 2 {
		t.Errorf("Unexpectedly pings for `bar`: %v (expected: 2)", barState.pings)
	}

	if fooState.requests != 2 {
		t.Errorf("Unexpectedly requests for `foo`: %v (expected: 2)", fooState.requests)
	}

	if barState.requests != 2 {
		t.Errorf("Unexpectedly requests for `bar`: %v (expected: 2)", barState.requests)
	}
}
