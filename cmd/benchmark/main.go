package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
)

const (
	PartPostFix   = "_parts"
	MsgIDHeader   = "MSG_ID"
	TotalPartsHdr = "TOTAL_PARTS"
	PartSizeHdr   = "PART_SIZE"
	PartIndexHdr  = "PART_INDEX"
	MultipartHdr  = "Multipart"
	DataSizeHdr   = "DATA_SIZE"
)

type ChunkedSender struct {
	nc        *nats.Conn
	chunkSize int
}

type SendResult struct {
	MsgID      string
	TotalParts int
	ChunkSize  int
	DataSize   int
	Duration   time.Duration
}

func NewChunkedSender(nc *nats.Conn, chunkSize int) *ChunkedSender {
	return &ChunkedSender{nc: nc, chunkSize: chunkSize}
}

func (cs *ChunkedSender) Send(subject string, data []byte) (*SendResult, error) {
	start := time.Now()
	dataSize := len(data)
	msgID := uuid.NewString()

	parts := dataSize / cs.chunkSize
	if dataSize%cs.chunkSize != 0 {
		parts++
	}

	if dataSize <= cs.chunkSize {
		header := nats.Header{}
		header.Set(MultipartHdr, "false")
		header.Set(MsgIDHeader, msgID)
		header.Set(DataSizeHdr, fmt.Sprintf("%d", dataSize))

		msg := &nats.Msg{Subject: subject, Header: header, Data: data}
		if err := cs.nc.PublishMsg(msg); err != nil {
			return nil, err
		}
		return &SendResult{MsgID: msgID, TotalParts: 1, ChunkSize: dataSize, DataSize: dataSize, Duration: time.Since(start)}, nil
	}

	header := nats.Header{}
	header.Set(MultipartHdr, "true")
	header.Set(MsgIDHeader, msgID)
	header.Set(TotalPartsHdr, fmt.Sprintf("%d", parts))
	header.Set(PartSizeHdr, fmt.Sprintf("%d", cs.chunkSize))
	header.Set(DataSizeHdr, fmt.Sprintf("%d", dataSize))

	if err := cs.nc.PublishMsg(&nats.Msg{Subject: subject, Header: header, Data: []byte{}}); err != nil {
		return nil, err
	}

	for i := range parts {
		startIdx := i * cs.chunkSize
		endIdx := min(startIdx+cs.chunkSize, dataSize)

		chunkHeader := nats.Header{}
		chunkHeader.Set(MsgIDHeader, msgID)
		chunkHeader.Set(PartIndexHdr, fmt.Sprintf("%d", i))
		chunkHeader.Set(TotalPartsHdr, fmt.Sprintf("%d", parts))

		if err := cs.nc.PublishMsg(&nats.Msg{Subject: subject + PartPostFix, Header: chunkHeader, Data: data[startIdx:endIdx]}); err != nil {
			return nil, err
		}
	}

	if err := cs.nc.Flush(); err != nil {
		return nil, err
	}

	return &SendResult{MsgID: msgID, TotalParts: parts, ChunkSize: cs.chunkSize, DataSize: dataSize, Duration: time.Since(start)}, nil
}

func (cs *ChunkedSender) SendConcurrent(subject string, data []byte, workers int) (*SendResult, error) {
	start := time.Now()
	dataSize := len(data)
	msgID := uuid.NewString()

	parts := dataSize / cs.chunkSize
	if dataSize%cs.chunkSize != 0 {
		parts++
	}

	if dataSize <= cs.chunkSize {
		return cs.Send(subject, data)
	}

	header := nats.Header{}
	header.Set(MultipartHdr, "true")
	header.Set(MsgIDHeader, msgID)
	header.Set(TotalPartsHdr, fmt.Sprintf("%d", parts))
	header.Set(PartSizeHdr, fmt.Sprintf("%d", cs.chunkSize))
	header.Set(DataSizeHdr, fmt.Sprintf("%d", dataSize))

	if err := cs.nc.PublishMsg(&nats.Msg{Subject: subject, Header: header, Data: []byte{}}); err != nil {
		return nil, err
	}

	type chunk struct {
		index int
		data  []byte
	}
	chunks := make(chan chunk, parts)

	for i := range parts {
		startIdx := i * cs.chunkSize
		endIdx := min(startIdx+cs.chunkSize, dataSize)
		chunks <- chunk{index: i, data: data[startIdx:endIdx]}
	}
	close(chunks)

	var wg sync.WaitGroup
	errChan := make(chan error, workers)

	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for c := range chunks {
				chunkHeader := nats.Header{}
				chunkHeader.Set(MsgIDHeader, msgID)
				chunkHeader.Set(PartIndexHdr, fmt.Sprintf("%d", c.index))
				chunkHeader.Set(TotalPartsHdr, fmt.Sprintf("%d", parts))

				if err := cs.nc.PublishMsg(&nats.Msg{Subject: subject + PartPostFix, Header: chunkHeader, Data: c.data}); err != nil {
					errChan <- err
					return
				}
			}
		}()
	}

	wg.Wait()
	close(errChan)

	if err := <-errChan; err != nil {
		return nil, err
	}

	if err := cs.nc.Flush(); err != nil {
		return nil, err
	}

	return &SendResult{MsgID: msgID, TotalParts: parts, ChunkSize: cs.chunkSize, DataSize: dataSize, Duration: time.Since(start)}, nil
}

type BenchmarkResult struct {
	TestName        string
	TestType        string
	DataSizeBytes   int
	ChunkSizeBytes  int
	Workers         int
	Chunks          int
	Iterations      int
	TotalDurationMs float64
	AvgDurationMs   float64
	MinDurationMs   float64
	MaxDurationMs   float64
	P50DurationMs   float64
	P95DurationMs   float64
	P99DurationMs   float64
	ThroughputMBs   float64
	AllocBytes      uint64
}

func generateTestData(size int) []byte {
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i % 256)
	}
	return data
}

func runBenchmark(nc *nats.Conn, testType string, dataSize, chunkSize, workers, iterations int) BenchmarkResult {
	data := generateTestData(dataSize)
	sender := NewChunkedSender(nc, chunkSize)
	subject := fmt.Sprintf("bench.%s.%d.%d.%d", testType, dataSize, chunkSize, time.Now().UnixNano())

	durations := make([]time.Duration, 0, iterations)

	var memStatsBefore, memStatsAfter runtime.MemStats
	runtime.ReadMemStats(&memStatsBefore)

	totalStart := time.Now()

	for i := 0; i < iterations; i++ {
		var result *SendResult
		var err error

		if workers > 1 {
			result, err = sender.SendConcurrent(subject, data, workers)
		} else {
			result, err = sender.Send(subject, data)
		}

		if err != nil {
			log.Printf("Benchmark error: %v", err)
			continue
		}
		durations = append(durations, result.Duration)
	}

	totalDuration := time.Since(totalStart)
	runtime.ReadMemStats(&memStatsAfter)

	if len(durations) == 0 {
		return BenchmarkResult{}
	}

	sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })

	var sum time.Duration
	for _, d := range durations {
		sum += d
	}

	chunks := dataSize / chunkSize
	if dataSize%chunkSize != 0 {
		chunks++
	}

	totalBytes := float64(dataSize * iterations)
	throughputMBs := (totalBytes / 1024 / 1024) / totalDuration.Seconds()

	testName := fmt.Sprintf("%s_data%dKB_chunk%dKB", testType, dataSize/1024, chunkSize/1024)
	if workers > 1 {
		testName = fmt.Sprintf("%s_workers%d", testName, workers)
	}

	return BenchmarkResult{
		TestName:        testName,
		TestType:        testType,
		DataSizeBytes:   dataSize,
		ChunkSizeBytes:  chunkSize,
		Workers:         workers,
		Chunks:          chunks,
		Iterations:      len(durations),
		TotalDurationMs: float64(totalDuration.Milliseconds()),
		AvgDurationMs:   float64(sum.Milliseconds()) / float64(len(durations)),
		MinDurationMs:   float64(durations[0].Microseconds()) / 1000.0,
		MaxDurationMs:   float64(durations[len(durations)-1].Microseconds()) / 1000.0,
		P50DurationMs:   float64(durations[len(durations)/2].Microseconds()) / 1000.0,
		P95DurationMs:   float64(durations[int(float64(len(durations))*0.95)].Microseconds()) / 1000.0,
		P99DurationMs:   float64(durations[int(float64(len(durations))*0.99)].Microseconds()) / 1000.0,
		ThroughputMBs:   throughputMBs,
		AllocBytes:      memStatsAfter.TotalAlloc - memStatsBefore.TotalAlloc,
	}
}

func runDirectBenchmark(nc *nats.Conn, dataSize, iterations int) BenchmarkResult {
	data := generateTestData(dataSize)
	subject := fmt.Sprintf("bench.direct.%d.%d", dataSize, time.Now().UnixNano())

	durations := make([]time.Duration, 0, iterations)

	var memStatsBefore, memStatsAfter runtime.MemStats
	runtime.ReadMemStats(&memStatsBefore)

	totalStart := time.Now()

	for i := 0; i < iterations; i++ {
		start := time.Now()
		if err := nc.Publish(subject, data); err != nil {
			log.Printf("Direct publish error: %v", err)
			continue
		}
		durations = append(durations, time.Since(start))
	}
	nc.Flush()

	totalDuration := time.Since(totalStart)
	runtime.ReadMemStats(&memStatsAfter)

	if len(durations) == 0 {
		return BenchmarkResult{}
	}

	sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })

	var sum time.Duration
	for _, d := range durations {
		sum += d
	}

	totalBytes := float64(dataSize * iterations)
	throughputMBs := (totalBytes / 1024 / 1024) / totalDuration.Seconds()

	return BenchmarkResult{
		TestName:        fmt.Sprintf("direct_data%dKB", dataSize/1024),
		TestType:        "direct",
		DataSizeBytes:   dataSize,
		ChunkSizeBytes:  dataSize,
		Workers:         1,
		Chunks:          1,
		Iterations:      len(durations),
		TotalDurationMs: float64(totalDuration.Milliseconds()),
		AvgDurationMs:   float64(sum.Milliseconds()) / float64(len(durations)),
		MinDurationMs:   float64(durations[0].Microseconds()) / 1000.0,
		MaxDurationMs:   float64(durations[len(durations)-1].Microseconds()) / 1000.0,
		P50DurationMs:   float64(durations[len(durations)/2].Microseconds()) / 1000.0,
		P95DurationMs:   float64(durations[int(float64(len(durations))*0.95)].Microseconds()) / 1000.0,
		P99DurationMs:   float64(durations[int(float64(len(durations))*0.99)].Microseconds()) / 1000.0,
		ThroughputMBs:   throughputMBs,
		AllocBytes:      memStatsAfter.TotalAlloc - memStatsBefore.TotalAlloc,
	}
}

func main() {
	natsURL := flag.String("nats", nats.DefaultURL, "NATS server URL")
	iterations := flag.Int("iterations", 100, "Number of iterations per benchmark")
	outputFile := flag.String("output", "", "Output CSV file (stdout if empty)")
	flag.Parse()

	nc, err := nats.Connect(*natsURL)
	if err != nil {
		log.Fatalf("Failed to connect to NATS: %v", err)
	}
	defer nc.Close()

	fmt.Fprintf(os.Stderr, "Connected to NATS at %s\n", *natsURL)
	fmt.Fprintf(os.Stderr, "Running benchmarks with %d iterations each...\n", *iterations)

	dataSizes := []int{
		64 * 1024,
		256 * 1024,
		1 * 1024 * 1024,
		4 * 1024 * 1024,
		8 * 1024 * 1024,
		16 * 1024 * 1024,
		32 * 1024 * 1024,
	}

	chunkSizes := []int{
		64 * 1024,
		128 * 1024,
		256 * 1024,
		512 * 1024,
		768 * 1024,
	}

	workerCounts := []int{1, 2, 4, 8}

	var results []BenchmarkResult

	fmt.Fprintf(os.Stderr, "\n=== Direct Publish Benchmarks ===\n")
	for _, dataSize := range dataSizes[:3] {
		fmt.Fprintf(os.Stderr, "  Direct publish %dKB...\n", dataSize/1024)
		result := runDirectBenchmark(nc, dataSize, *iterations)
		results = append(results, result)
	}

	fmt.Fprintf(os.Stderr, "\n=== Sequential Send Benchmarks ===\n")
	for _, dataSize := range dataSizes {
		for _, chunkSize := range chunkSizes {
			if chunkSize > dataSize {
				continue
			}
			fmt.Fprintf(os.Stderr, "  Send %dKB with %dKB chunks...\n", dataSize/1024, chunkSize/1024)
			result := runBenchmark(nc, "sequential", dataSize, chunkSize, 1, *iterations)
			results = append(results, result)
		}
	}

	fmt.Fprintf(os.Stderr, "\n=== Concurrent Send Benchmarks ===\n")
	for _, dataSize := range dataSizes {
		for _, chunkSize := range chunkSizes {
			if chunkSize > dataSize {
				continue
			}
			for _, workers := range workerCounts {
				if workers == 1 {
					continue
				}
				fmt.Fprintf(os.Stderr, "  Concurrent send %dKB with %dKB chunks, %d workers...\n", dataSize/1024, chunkSize/1024, workers)
				result := runBenchmark(nc, "concurrent", dataSize, chunkSize, workers, *iterations)
				results = append(results, result)
			}
		}
	}

	var writer *csv.Writer
	if *outputFile != "" {
		f, err := os.Create(*outputFile)
		if err != nil {
			log.Fatalf("Failed to create output file: %v", err)
		}
		defer f.Close()
		writer = csv.NewWriter(f)
	} else {
		writer = csv.NewWriter(os.Stdout)
	}

	header := []string{
		"test_name",
		"test_type",
		"data_size_bytes",
		"chunk_size_bytes",
		"workers",
		"chunks",
		"iterations",
		"total_duration_ms",
		"avg_duration_ms",
		"min_duration_ms",
		"max_duration_ms",
		"p50_duration_ms",
		"p95_duration_ms",
		"p99_duration_ms",
		"throughput_mb_s",
		"alloc_bytes",
	}
	writer.Write(header)

	for _, r := range results {
		row := []string{
			r.TestName,
			r.TestType,
			fmt.Sprintf("%d", r.DataSizeBytes),
			fmt.Sprintf("%d", r.ChunkSizeBytes),
			fmt.Sprintf("%d", r.Workers),
			fmt.Sprintf("%d", r.Chunks),
			fmt.Sprintf("%d", r.Iterations),
			fmt.Sprintf("%.3f", r.TotalDurationMs),
			fmt.Sprintf("%.3f", r.AvgDurationMs),
			fmt.Sprintf("%.3f", r.MinDurationMs),
			fmt.Sprintf("%.3f", r.MaxDurationMs),
			fmt.Sprintf("%.3f", r.P50DurationMs),
			fmt.Sprintf("%.3f", r.P95DurationMs),
			fmt.Sprintf("%.3f", r.P99DurationMs),
			fmt.Sprintf("%.3f", r.ThroughputMBs),
			fmt.Sprintf("%d", r.AllocBytes),
		}
		writer.Write(row)
	}

	writer.Flush()

	if *outputFile != "" {
		fmt.Fprintf(os.Stderr, "\nResults written to %s\n", *outputFile)
	}

	fmt.Fprintf(os.Stderr, "\nBenchmark complete. %d tests executed.\n", len(results))
}
