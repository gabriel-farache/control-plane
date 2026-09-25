package app

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	agentserver "github.com/dcm-project/control-plane/internal/agent/api/server"
	agenthandlers "github.com/dcm-project/control-plane/internal/agent/handlers/v1alpha1"
	agenthealthcheck "github.com/dcm-project/control-plane/internal/agent/healthcheck"
	agentservice "github.com/dcm-project/control-plane/internal/agent/service"
	agentstore "github.com/dcm-project/control-plane/internal/agent/store/agent"
	"github.com/dcm-project/control-plane/internal/auth"
	authservice "github.com/dcm-project/control-plane/internal/auth/service"
	authstore "github.com/dcm-project/control-plane/internal/auth/store"
	catalogserver "github.com/dcm-project/control-plane/internal/catalog/api/server"
	catalogconfig "github.com/dcm-project/control-plane/internal/catalog/config"
	cataloghandlers "github.com/dcm-project/control-plane/internal/catalog/handlers/v1alpha1"
	catalogplacement "github.com/dcm-project/control-plane/internal/catalog/placement"
	catalogservice "github.com/dcm-project/control-plane/internal/catalog/service"
	catalogstore "github.com/dcm-project/control-plane/internal/catalog/store"
	gitopsserver "github.com/dcm-project/control-plane/internal/gitops/api/server"
	gitopshandlers "github.com/dcm-project/control-plane/internal/gitops/handlers/v1alpha1"
	gitopsservice "github.com/dcm-project/control-plane/internal/gitops/service"
	gitopsstore "github.com/dcm-project/control-plane/internal/gitops/store"
	placementagent "github.com/dcm-project/control-plane/internal/placement/agent"
	placementlogging "github.com/dcm-project/control-plane/internal/placement/logging"
	placementpolicy "github.com/dcm-project/control-plane/internal/placement/policy"
	placementservice "github.com/dcm-project/control-plane/internal/placement/service"
	placementsprm "github.com/dcm-project/control-plane/internal/placement/sprm"
	placementstore "github.com/dcm-project/control-plane/internal/placement/store"
	policyserver "github.com/dcm-project/control-plane/internal/policy/api/server"
	policyhandlers "github.com/dcm-project/control-plane/internal/policy/handlers/v1alpha1"
	policylogging "github.com/dcm-project/control-plane/internal/policy/logging"
	policyopa "github.com/dcm-project/control-plane/internal/policy/opa"
	policyservice "github.com/dcm-project/control-plane/internal/policy/service"
	policystore "github.com/dcm-project/control-plane/internal/policy/store"
	sprmserver "github.com/dcm-project/control-plane/internal/sp/api/resource_manager"
	spcleanup "github.com/dcm-project/control-plane/internal/sp/cleanup"
	spconfig "github.com/dcm-project/control-plane/internal/sp/config"
	spconsumer "github.com/dcm-project/control-plane/internal/sp/consumer"
	sprmhandler "github.com/dcm-project/control-plane/internal/sp/handlers/resource_manager"
	splogging "github.com/dcm-project/control-plane/internal/sp/logging"
	"github.com/dcm-project/control-plane/internal/sp/messaging"
	sppending "github.com/dcm-project/control-plane/internal/sp/pending"
	sprmsvc "github.com/dcm-project/control-plane/internal/sp/service/resource_manager"
	spstore "github.com/dcm-project/control-plane/internal/sp/store"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

const gracefulShutdownTimeout = 5 * time.Second

func Run() int {
	cfg, err := LoadConfig()
	if err != nil {
		slog.Error("Failed to load configuration", "error", err)
		return 1
	}

	logger := initLogger(cfg.Service.LogLevel)
	slog.SetDefault(logger)
	placementlogging.Init(cfg.Service.LogLevel)
	policylogging.Init(cfg.Service.LogLevel)
	splogging.Init(cfg.Service.LogLevel)

	slog.Info("Configuration loaded",
		"bind_address", cfg.Service.BindAddress,
		"db_type", cfg.Database.Type,
		"db_host", cfg.Database.Hostname,
		"db_name", cfg.Database.Name,
		"log_level", cfg.Service.LogLevel,
	)

	db, err := openDB(cfg)
	if err != nil {
		slog.Error("Failed to initialize database", "error", err)
		return 1
	}
	defer func() { _ = closeDB(db) }()

	authDataStore := authstore.NewStore(db)
	authSvc := authservice.NewService(authDataStore, cfg.Auth.AdminSubject, logger)

	catalogDataStore := catalogstore.NewStore(db, logger)
	gitopsDataStore := gitopsstore.NewStore(db)
	policyDataStore := policystore.NewStore(db)
	placementDataStore := placementstore.NewStore(db)
	spDataStore := spstore.NewStore(db)

	agentSt := agentstore.NewAgent(db)
	agentSvc := agentservice.NewAgentService(agentSt, cfg.Agent.ConsumerLagThreshold)
	agentClient := placementagent.NewServiceClient(agentSt)

	opaEngine := policyopa.NewEngine()
	policyService := policyservice.NewPolicyService(policyDataStore, opaEngine)
	evaluationService := policyservice.NewEvaluationService(policyDataStore.Policy(), opaEngine)
	if err := policyService.CompileAll(context.Background()); err != nil {
		slog.Error("Failed to compile policies on startup", "error", err)
		return 1
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	var publisher *messaging.Publisher
	checkers := []Checker{NewPostgresChecker(db)}

	var agentConn *messaging.Connection
	if !cfg.NATS.Disabled {
		var err error
		agentConn, err = messaging.Connect(ctx, cfg.NATS.URL)
		if err != nil {
			slog.Error("Failed to initialize agent messaging", "error", err)
			return 1
		}
		defer agentConn.Close()
		publisher = agentConn.Publisher
	}

	policyClient := placementpolicy.NewServiceClient(evaluationService)
	spInstanceService := sprmsvc.NewInstanceService(spDataStore, publisher, agentSt)
	sprmClient := placementsprm.NewServiceClient(spInstanceService)
	placementService := placementservice.NewPlacementService(
		placementDataStore, policyClient, sprmClient,
		placementservice.WithAgentClient(agentClient),
	)

	if agentConn != nil {
		responseConsumer := spconsumer.NewResponseConsumer(
			agentConn.JetStream,
			spDataStore,
			agentSt,
			cfg.Agent.ResponseMaxDeliver,
			cfg.Agent.ResponseAckWait,
			spconsumer.SetPlacementDeletionHandler(placementService.OnResourceDeleted),
			spconsumer.SetPlacementAgentErrorHandler(placementService.OnAgentError),
		)
		if err := responseConsumer.Start(ctx); err != nil {
			slog.Error("Failed to start agent response consumer", "error", err)
			return 1
		}
		defer responseConsumer.Stop()

		statusConsumer, err := spconsumer.New(cfg.NATS.URL, cfg.NATS.Subject, spDataStore,
			spconsumer.SetStreamName(cfg.NATS.StreamName),
			spconsumer.SetConsumerName(cfg.NATS.ConsumerName),
			spconsumer.SetPlacementStatusHandlers(
				placementService.OnResourceRunning,
				nil,
				placementService.OnResourceFailed,
			),
		)
		if err != nil {
			slog.Error("Failed to initialize status consumer", "error", err)
			return 1
		}
		if err := statusConsumer.Start(ctx); err != nil {
			slog.Error("Failed to start status consumer", "error", err)
			return 1
		}
		defer statusConsumer.Stop()
		checkers = append(checkers, NewNATSChecker(statusConsumer))
	}
	pmClient, err := buildPlacementClient(cfg, placementService, logger)
	if err != nil {
		slog.Error("Failed to initialize placement client", "error", err)
		return 1
	}

	seedCfg := catalogconfig.SeedConfig{
		RegionDefault: cfg.Seed.RegionDefault,
		RegionEnum:    cfg.Seed.RegionEnum,
	}
	catalogSvc, err := catalogservice.NewService(catalogDataStore, pmClient, seedCfg, logger)
	if err != nil {
		slog.Error("Failed to create catalog service", "error", err)
		return 1
	}

	if err := authSvc.Seed(ctx); err != nil {
		slog.Error("Failed to seed auth database", "error", err)
		return 1
	}

	seedCtx := auth.WithActorInfo(ctx, auth.ActorInfo{
		ActorType: "system",
	})
	if err := catalogSvc.Seed(seedCtx); err != nil {
		slog.Error("Failed to seed catalog database", "error", err)
		return 1
	}

	agentSweep := sppending.NewSweep(db, publisher, agentSt, placementService, cfg.Agent.PendingRequestTimeout, cfg.Agent.QueuedRequestTimeout, cfg.Agent.SweepInterval, cfg.Agent.PendingRequestMaxRetries)
	agentSweep.Start(ctx)
	defer agentSweep.Stop()

	agentHealthMonitor := agenthealthcheck.NewMonitor(agentSt, cfg.Agent.HeartbeatTimeout, cfg.SP.HealthCheckInterval)
	agentHealthMonitor.Start(ctx)
	defer agentHealthMonitor.Stop()

	cleanupScheduler := spcleanup.NewScheduler(spDataStore, publisher, agentSt, &spconfig.CleanupConfig{
		Interval:   cfg.SP.CleanupInterval,
		MaxRetries: cfg.SP.CleanupMaxRetries,
		Timeout:    cfg.SP.CleanupTimeout,
	})
	cleanupScheduler.Start(ctx)
	defer cleanupScheduler.Stop()

	var authMiddleware func(http.Handler) http.Handler
	if cfg.Auth.Disabled {
		authMiddleware = auth.DisabledMiddleware(logger)
	} else {
		if cfg.Auth.ProxySecret == "" && cfg.Auth.IssuerURL == "" {
			slog.Error("AUTH_PROXY_SECRET or AUTH_ISSUER_URL is required when authentication is enabled")
			return 1
		}
		actorCache := auth.NewActorCache(cfg.Auth.CacheTTL)
		mwCfg := auth.MiddlewareConfig{
			ProxySecret: cfg.Auth.ProxySecret,
			Resolver:    authSvc,
			Cache:       actorCache,
			Logger:      logger,
		}
		if cfg.Auth.IssuerURL != "" {
			jwtValidator, err := auth.NewOIDCValidator(ctx, cfg.Auth.IssuerURL, cfg.Auth.Audience)
			if err != nil {
				slog.Error("Failed to initialize OIDC JWT validator", "error", err, "issuer", cfg.Auth.IssuerURL)
				return 1
			}
			mwCfg.JWTValidator = jwtValidator
			if cfg.Auth.Audience == "" {
				slog.Warn("AUTH_JWT_AUDIENCE is empty - audience validation is disabled, tokens from any client in this issuer realm will be accepted", "issuer", cfg.Auth.IssuerURL, "component", "auth")
			}
			slog.Info("JWT bearer validation enabled", "issuer", cfg.Auth.IssuerURL, "component", "auth")
		}
		authMiddleware = auth.Middleware(mwCfg)
	}

	gitopsSvc := gitopsservice.NewGitRepositoryService(gitopsDataStore)

	router, err := newRouter(authMiddleware, RouteHandlers{
		Agent:   agenthandlers.NewHandler(agentSvc),
		Catalog: cataloghandlers.NewHandler(catalogSvc, logger),
		Gitops:  gitopshandlers.NewHandler(gitopsSvc),
		Policy:  policyhandlers.NewPolicyHandler(policyService),
		SPRM:    sprmhandler.NewHandler(spInstanceService),
	}, checkers...)
	if err != nil {
		slog.Error("Failed to configure HTTP router", "error", err)
		return 1
	}

	listener, err := net.Listen("tcp", cfg.Service.BindAddress)
	if err != nil {
		slog.Error("Failed to create listener", "error", err)
		return 1
	}

	srv := &http.Server{
		Handler:           router,
		ReadHeaderTimeout: cfg.Service.ReadHeaderTimeout,
		ReadTimeout:       cfg.Service.ReadTimeout,
		WriteTimeout:      cfg.Service.WriteTimeout,
		IdleTimeout:       cfg.Service.IdleTimeout,
		MaxHeaderBytes:    cfg.Service.MaxHeaderBytes,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), gracefulShutdownTimeout)
		defer shutdownCancel()
		srv.SetKeepAlivesEnabled(false)
		slog.Info("Shutting down server")
		_ = srv.Shutdown(shutdownCtx)
	}()

	slog.Info("Starting control-plane", "address", listener.Addr().String())
	if err := srv.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("Server failed", "error", err)
		return 1
	}
	return 0
}

type RouteHandlers struct {
	Agent   agentserver.StrictServerInterface
	Catalog catalogserver.StrictServerInterface
	Gitops  gitopsserver.StrictServerInterface
	Policy  policyserver.StrictServerInterface
	SPRM    sprmserver.StrictServerInterface
}

func newRouter(authMW func(http.Handler) http.Handler, h RouteHandlers, checkers ...Checker) (chi.Router, error) {
	validators, err := newOpenAPIValidators()
	if err != nil {
		return nil, err
	}

	router := chi.NewRouter()
	router.Use(middleware.RequestID)
	router.Use(middleware.Recoverer)
	router.Use(authMW)
	router.Use(validators.middleware())

	const baseURL = "/api/v1alpha1"

	registerMonolithHealth(router, checkers...)

	agentserver.HandlerFromMuxWithBaseURL(
		agentserver.NewStrictHandler(h.Agent, nil),
		router,
		baseURL,
	)
	catalogserver.HandlerFromMuxWithBaseURL(
		catalogserver.NewStrictHandler(h.Catalog, nil),
		router,
		baseURL,
	)
	policyserver.HandlerFromMuxWithBaseURL(
		policyserver.NewStrictHandler(h.Policy, nil),
		router,
		baseURL,
	)
	gitopsserver.HandlerFromMuxWithBaseURL(
		gitopsserver.NewStrictHandler(h.Gitops, nil),
		router,
		baseURL,
	)

	apiRouter := chi.NewRouter()
	sprmserver.HandlerFromMux(
		sprmserver.NewStrictHandler(h.SPRM, nil),
		apiRouter,
	)
	router.Mount(baseURL, apiRouter)

	return router, nil
}

func buildPlacementClient(cfg *Config, svc *placementservice.PlacementService, logger *slog.Logger) (catalogplacement.Client, error) {
	if url := strings.TrimSpace(cfg.Wiring.PlacementManagerURL); url != "" {
		return catalogplacement.NewClient(url, cfg.Wiring.PlacementManagerTimeout, logger)
	}
	return catalogplacement.NewLocalClient(svc, logger), nil
}

func initLogger(level string) *slog.Logger {
	var logLevel slog.Level
	switch strings.ToLower(level) {
	case "debug":
		logLevel = slog.LevelDebug
	case "info":
		logLevel = slog.LevelInfo
	case "warn", "warning":
		logLevel = slog.LevelWarn
	case "error":
		logLevel = slog.LevelError
	default:
		logLevel = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel}))
}
