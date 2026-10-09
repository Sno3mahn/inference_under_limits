// Command bench runs the hot-path engine against its in-process provider and reports what happened.
//
//	go run ./cmd/bench -target 300000 -layout pinned
//
// Each configuration is run -repeats times. The run with the median completion rate is the
// headline; all runs are saved to the JSON file so the spread is visible.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sort"

	"github.com/Sno3mahn/inference-under-limits/go/engine"
)

type output struct {
	Name        string           `json:"name"`
	Repeats     int              `json:"repeats"`
	MedianIndex int              `json:"median_index"`
	CompletedPS []float64        `json:"completed_per_s_by_run"`
	Median      *engine.Result   `json:"median"`
	Runs        []*engine.Result `json:"runs"`
}

func main() {
	cfg := engine.DefaultConfig()
	repeats := flag.Int("repeats", 3, "runs per configuration; the median run is the headline")
	cpuProfile := flag.String("cpuprofile", "", "write a CPU profile to this file")
	out := flag.String("out", "", "JSON output path (default ../runs/go/<name>.json)")
	name := flag.String("name", "", "run name (default <layout>-<target>)")

	flag.Float64Var(&cfg.Target, "target", cfg.Target, "requests/s the limits are sized for")
	flag.Float64Var(&cfg.Offer, "offer", cfg.Offer, "offered load as a multiple of target")
	flag.BoolVar(&cfg.Ceiling, "ceiling", cfg.Ceiling, "take the limits out of the way to find the CPU ceiling")
	flag.IntVar(&cfg.Models, "models", cfg.Models, "number of models")
	flag.StringVar(&cfg.Layout, "layout", cfg.Layout, "pinned (a model on one shard) or spread (a model on every shard)")
	flag.IntVar(&cfg.Shards, "shards", cfg.Shards, "shard goroutines")
	flag.IntVar(&cfg.Generators, "generators", cfg.Generators, "load generator goroutines")
	flag.DurationVar(&cfg.Duration, "duration", cfg.Duration, "load duration")
	flag.DurationVar(&cfg.Warmup, "warmup", cfg.Warmup, "excluded from the headline rate")
	flag.DurationVar(&cfg.P50, "p50", cfg.P50, "provider median latency")
	flag.DurationVar(&cfg.P99, "p99", cfg.P99, "provider p99 latency")
	flag.Float64Var(&cfg.Transient, "transient", cfg.Transient, "fraction of calls that fail transiently")
	flag.Float64Var(&cfg.Permanent, "permanent", cfg.Permanent, "fraction of calls that fail permanently")
	flag.IntVar(&cfg.Tokens, "tokens", cfg.Tokens, "tokens per request")
	flag.Float64Var(&cfg.QueueS, "queue-s", cfg.QueueS, "per-lane queue bound, in seconds of the lane's rate")
	flag.Uint64Var(&cfg.Seed, "seed", cfg.Seed, "random seed")
	flag.Parse()

	if *repeats < 1 {
		fatal("repeats must be >= 1")
	}
	if *name == "" {
		*name = fmt.Sprintf("%s-%.0f", cfg.Layout, cfg.Target)
		if cfg.Ceiling {
			*name += "-ceiling"
		}
	}

	fmt.Printf("%s: %d run(s) of %s (warm-up %s), %d shards, GOMAXPROCS %d\n",
		*name, *repeats, cfg.Duration, cfg.Warmup, cfg.Shards, runtime.GOMAXPROCS(0))

	if *cpuProfile != "" { // records where CPU time goes, across all runs; read with `go tool pprof`
		f, err := os.Create(*cpuProfile)
		if err != nil {
			fatal(err.Error())
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			fatal(err.Error())
		}
	}

	var runs []*engine.Result
	for i := range *repeats {
		runtime.GC() // start each run from a clean heap
		r, err := engine.Run(cfg)
		if err != nil {
			fatal(err.Error())
		}
		runs = append(runs, r)
		check := verdict(r.ProviderPassed)
		if cfg.Ceiling {
			check = "n/a"
		}
		fmt.Printf("  run %d: completed %s/s of %s/s offered, provider check %s, accounting %s\n", i+1,
			si(r.CompletedPerS), si(r.OfferedPerS), check, verdict(r.AccountingOK))
	}
	if *cpuProfile != "" {
		pprof.StopCPUProfile()
		fmt.Println("cpu profile:", *cpuProfile)
	}

	order := make([]int, len(runs))
	rates := make([]float64, len(runs))
	for i := range runs {
		order[i], rates[i] = i, runs[i].CompletedPerS
	}
	sort.Slice(order, func(a, b int) bool { return rates[order[a]] < rates[order[b]] })
	median := order[len(order)/2]

	summarize(runs[median], rates)

	path := *out
	if path == "" {
		path = filepath.Join("..", "runs", "go", *name+".json")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		fatal(err.Error())
	}
	data, err := json.MarshalIndent(output{Name: *name, Repeats: *repeats, MedianIndex: median,
		CompletedPS: rates, Median: runs[median], Runs: runs}, "", "  ")
	if err != nil {
		fatal(err.Error())
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		fatal(err.Error())
	}
	fmt.Println("saved", path)

	for _, r := range runs {
		if !r.ProviderPassed || !r.AccountingOK {
			os.Exit(1)
		}
	}
}

func summarize(r *engine.Result, rates []float64) {
	c := r.Config
	fmt.Printf("\nmedian run (%s, %d models, %d lanes)\n", c.Layout, c.Models, r.Limits.Lanes)
	fmt.Printf("  offered      %s/s   (target %s/s x %.2f)\n", si(r.OfferedPerS), si(c.Target), c.Offer)
	fmt.Printf("  completed    %s/s   over t=%s..%s; %.0f%% of target; all runs: %s\n",
		si(r.CompletedPerS), c.Warmup, c.Duration, 100*r.CompletedPerS/c.Target, siList(rates))
	if c.Ceiling {
		fmt.Printf("  caps         none: limits disabled to find the CPU ceiling\n")
	} else {
		fmt.Printf("  caps sum to  %s/s   (provider limits x 97%%)\n", si(r.Limits.TotalCapPerS))
	}
	fmt.Printf("  requests     generated %s, accepted %s, rejected %s (queue full), %d never fit\n",
		si(float64(r.Generated)), si(float64(r.Accepted)), si(float64(r.Rejected)), r.NeverFits)
	fmt.Printf("  outcomes     succeeded %s, failed %s; %s provider calls, %s retries (%.1f%%)\n",
		si(float64(r.Succeeded)), si(float64(r.Failed)), si(float64(r.Calls)), si(float64(r.Retries)),
		100*float64(r.Retries)/float64(max(r.Calls, 1)))
	fmt.Printf("  latency      p50 %.0f ms, p99 %.0f ms, p99.9 %.0f ms (submit to final, includes queueing)\n",
		r.LatencyMs["p50"], r.LatencyMs["p99"], r.LatencyMs["p999"])
	if c.Ceiling {
		fmt.Printf("  provider     limit check not applicable (no limits in ceiling mode)\n")
	} else {
		fmt.Printf("  provider     busiest 60 s window = %.1f%% of the limit; %s\n",
			100*r.MaxPeakFraction, verdict(r.ProviderPassed))
	}
	fmt.Printf("  accounting   %s; pending at stop %v\n", verdict(r.AccountingOK), r.AtStop)
	fmt.Printf("  cost         %.1f cores busy, %d GCs (%.1f ms paused in total)\n", r.CoresUsed, r.GCCount, r.GCPauseMs)
	if r.InboxDropped > 0 {
		fmt.Printf("  WARNING      %d requests could not be handed to a shard: the generator outran it\n", r.InboxDropped)
	}
}

func verdict(ok bool) string {
	if ok {
		return "PASS"
	}
	return "FAIL"
}

// si formats a rate or count: 1.23M, 456k, 789.
func si(v float64) string {
	switch {
	case v >= 1e6:
		return fmt.Sprintf("%.2fM", v/1e6)
	case v >= 1e3:
		return fmt.Sprintf("%.1fk", v/1e3)
	}
	return fmt.Sprintf("%.0f", v)
}

func siList(vs []float64) string {
	s := ""
	for i, v := range vs {
		if i > 0 {
			s += ", "
		}
		s += si(v)
	}
	return s
}

func fatal(msg string) {
	fmt.Fprintln(os.Stderr, "bench:", msg)
	os.Exit(2)
}
