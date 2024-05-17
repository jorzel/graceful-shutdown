package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/rs/zerolog"
)

func main() {
	logger, err := configureLogger("INFO")
	if err != nil {
		logger.Fatal().Err(err).Msg("Cannot configure logger")
	}
	ctx := logger.WithContext(context.Background())

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		logger.Fatal().Err(err).Msg("Cannot create filesystem watcher")
	}

	listener := NewFileListener(watcher, FileHandler{})
	go listener.Start(ctx, "./")

	// graceful shutdown
	terminateSignal := make(chan os.Signal, 1)
	signal.Notify(terminateSignal, os.Interrupt, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)

	<-terminateSignal
	logger.Info().Msg("Received termination signal. Shutting down ...")
	err = listener.Stop(ctx)
	if err != nil {
		logger.Fatal().Err(err).Msg("Cannot stop listener")
	}
}

type FileHandler struct{}

type FileListener struct {
	watcher         *fsnotify.Watcher
	handler         FileHandler
	ongoindHandlers sync.WaitGroup
}

func (dfh *FileHandler) Handle(ctx context.Context, event fsnotify.Event) {
	// Do something with the event
	time.Sleep(15 * time.Second)
}

func NewFileListener(
	watcher *fsnotify.Watcher, handler FileHandler,
) *FileListener {
	return &FileListener{
		watcher: watcher,
		handler: handler,
	}
}

// Start trigger FilesystemListener to listen on file events within sourcePath directory indefinetely
func (fl *FileListener) Start(ctx context.Context, sourcePath string) error {
	logger := zerolog.Ctx(ctx)
	logger.Info().Str("sourcePath", sourcePath).Msg("Filesystem listener started ...")

	err := fl.watcher.Add(sourcePath)
	if err != nil {
		return fmt.Errorf("cannot add %s path to filesystem listener: %w", sourcePath, err)
	}

	go fl.listen(ctx)

	return nil
}

// Stop listening on events
func (fl *FileListener) Stop(ctx context.Context) error {
	err := fl.watcher.Close()
	logger := zerolog.Ctx(ctx)

	logger.Info().Msg("Filesystem listener closed")
	logger.Info().Msg("Waiting for ongoing handlers to finish ...")
	fl.ongoindHandlers.Wait()
	logger.Info().Msg("All ongoing handlers finished")

	return err
}

func (fl *FileListener) listen(ctx context.Context) {
	logger := zerolog.Ctx(ctx)

	for {
		select {
		case event, ok := <-fl.watcher.Events:
			if !ok {
				logger.Info().Msg("File listener stopped listening")
				return
			}
			go fl.handle(ctx, event)

		case err, ok := <-fl.watcher.Errors:
			if !ok {
				logger.Info().Msg("File listener stopped listening")
				return
			}
			logger.Error().Err(err).Msg("File listener event error")
		}
	}
}

func (fl *FileListener) handle(ctx context.Context, event fsnotify.Event) {
	logger := zerolog.Ctx(ctx)
	logger.Info().Str("event", event.String()).Msg("File event received")

	fl.ongoindHandlers.Add(1)
	defer fl.ongoindHandlers.Done()

	fl.handler.Handle(ctx, event)

	logger.Info().Str("event", event.String()).Msg("File event handled")
}

func configureLogger(logLevel string) (zerolog.Logger, error) {
	parsed, err := zerolog.ParseLevel(strings.ToLower(logLevel))
	if err != nil {
		return zerolog.Logger{}, fmt.Errorf(
			"can't parse given log level: given - %s, err - %w",
			parsed,
			err,
		)
	}

	zerolog.TimeFieldFormat = zerolog.TimeFormatUnixMs
	zerolog.SetGlobalLevel(parsed)
	return zerolog.New(os.Stderr).With().Caller().Timestamp().Logger(), nil
}
