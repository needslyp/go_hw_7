package main

import (
	"context"
	"encoding/json"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"net"
	"strings"
	"sync"
	"time"
)

// НАПИСАННАЯ ВРУЧНУЮ РЕАЛИЗАЦИЯ
// Сгенерированные сообщения и gRPC-обёртки находятся в *.pb.go.
// Вся изменяемая информация сервера хранится здесь.
type microservice struct {
	UnimplementedBizServer
	UnimplementedAdminServer
	mu    sync.Mutex
	acl   map[string][]string
	logs  map[*logSubscription]struct{}
	stats map[*Stat]struct{}
}
type logSubscription struct {
	queue []*Event
	wake  chan struct{}
}

func StartMyMicroservice(ctx context.Context, listenAddr, aclData string) error {
	s := &microservice{logs: make(map[*logSubscription]struct{}), stats: make(map[*Stat]struct{})}
	if err := json.Unmarshal([]byte(aclData), &s.acl); err != nil {
		return err
	}
	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return err
	}
	server := grpc.NewServer(grpc.UnaryInterceptor(s.checkUnaryAccess), grpc.StreamInterceptor(s.checkStreamAccess))
	RegisterBizServer(server, s)
	RegisterAdminServer(server, s)
	done := make(chan struct{})
	go func() {
		defer close(done)
		server.Serve(listener)
	}()
	go func() {
		select {
		case <-ctx.Done():
			server.Stop()
		case <-done:
		}
	}()
	return nil
}

// Проверяем имя клиента и разрешения для полного пути метода.
func (s *microservice) authorize(ctx context.Context, method string) (string, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	consumers := md.Get("consumer")
	if len(consumers) != 1 || consumers[0] == "" {
		return "", status.Error(codes.Unauthenticated, "consumer is missing")
	}
	consumer := consumers[0]
	for _, allowedMethod := range s.acl[consumer] {
		if allowedMethod == method {
			return consumer, nil
		}
		if strings.HasSuffix(allowedMethod, "/*") {
			servicePrefix := strings.TrimSuffix(allowedMethod, "*")
			if strings.HasPrefix(method, servicePrefix) {
				return consumer, nil
			}
		}
	}
	return "", status.Error(codes.Unauthenticated, "access denied")
}

// record вызывается только под s.mu: очередь и счётчики меняются вместе.
func (s *microservice) record(ctx context.Context, consumer, method string) {
	host := ""
	if p, ok := peer.FromContext(ctx); ok {
		host = p.Addr.String()
	}
	event := &Event{Timestamp: time.Now().Unix(), Consumer: consumer, Method: method, Host: host}
	for sub := range s.logs {
		sub.queue = append(sub.queue, &Event{Timestamp: event.Timestamp, Consumer: event.Consumer, Method: event.Method, Host: event.Host})
		select {
		case sub.wake <- struct{}{}:
		default:
		}
	}
	for stat := range s.stats {
		stat.ByMethod[method]++
		stat.ByConsumer[consumer]++
	}
}
func (s *microservice) checkUnaryAccess(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	consumer, err := s.authorize(ctx, info.FullMethod)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.record(ctx, consumer, info.FullMethod)
	s.mu.Unlock()
	return handler(ctx, req)
}
func (s *microservice) checkStreamAccess(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	_, err := s.authorize(stream.Context(), info.FullMethod)
	if err != nil {
		return err
	}
	return handler(srv, stream)
}
func (s *microservice) Logging(_ *Nothing, stream Admin_LoggingServer) error {
	ctx := stream.Context()
	consumer, err := s.authorize(ctx, "/main.Admin/Logging")
	if err != nil {
		return err
	}
	sub := &logSubscription{wake: make(chan struct{}, 1)}
	s.mu.Lock()
	// Существующие логгеры видят подключение нового. Сам он его не получает.
	s.record(ctx, consumer, "/main.Admin/Logging")
	s.logs[sub] = struct{}{}
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.logs, sub)
		s.mu.Unlock()
	}()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-sub.wake:
			s.mu.Lock()
			// Забираем очередь; сетевую отправку выполняем после Unlock.
			events := sub.queue
			sub.queue = nil
			s.mu.Unlock()
			for _, event := range events {
				if err := stream.Send(event); err != nil {
					return err
				}
			}
		}
	}
}
func (s *microservice) Statistics(req *StatInterval, stream Admin_StatisticsServer) error {
	if req.IntervalSeconds == 0 || req.IntervalSeconds > uint64((1<<63-1)/int64(time.Second)) {
		return status.Error(codes.InvalidArgument, "invalid statistics interval")
	}
	ctx := stream.Context()
	consumer, err := s.authorize(ctx, "/main.Admin/Statistics")
	if err != nil {
		return err
	}
	stat := &Stat{ByMethod: make(map[string]uint64), ByConsumer: make(map[string]uint64)}
	s.mu.Lock()
	s.record(ctx, consumer, "/main.Admin/Statistics")
	s.stats[stat] = struct{}{}
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.stats, stat)
		s.mu.Unlock()
	}()
	ticker := time.NewTicker(time.Duration(req.IntervalSeconds) * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			s.mu.Lock()
			// Отчёт получает старые карты, новые запросы — новые пустые карты.
			snapshot := &Stat{Timestamp: time.Now().Unix(), ByMethod: stat.ByMethod, ByConsumer: stat.ByConsumer}
			stat.ByMethod = make(map[string]uint64)
			stat.ByConsumer = make(map[string]uint64)
			s.mu.Unlock()
			if err := stream.Send(snapshot); err != nil {
				return err
			}
		}
	}
}

// Бизнес-методы по условию только возвращают пустой ответ.
func (s *microservice) Check(ctx context.Context, req *Nothing) (*Nothing, error) {
	return &Nothing{}, nil
}
func (s *microservice) Add(ctx context.Context, req *Nothing) (*Nothing, error) {
	return &Nothing{}, nil
}
func (s *microservice) Test(ctx context.Context, req *Nothing) (*Nothing, error) {
	return &Nothing{}, nil
}
