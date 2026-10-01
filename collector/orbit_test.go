package collector

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/cern-eos/eos_exporter/eosclient"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
)

type emptyOrbitCollector struct{}

func (emptyOrbitCollector) Describe(chan<- *prometheus.Desc) {}
func (emptyOrbitCollector) Collect(chan<- prometheus.Metric) {}

func orbitTestCollector(t *testing.T) *OrbitCollector {
	t.Helper()
	o := NewOrbitCollector(&CollectorOpts{Cluster: "test"})
	o.fetchRole = func(context.Context) (*eosclient.OrbitMGMRole, error) {
		return &eosclient.OrbitMGMRole{Leader: true, MGMID: "mgm.example:1094", Version: "5.4.12", Release: "1"}, nil
	}
	o.fetchFleet = func(context.Context) (*eosclient.OrbitFleet, error) {
		return &eosclient.OrbitFleet{
			CollectedAt: time.Unix(1700000000, 0),
			Nodes:       []*eosclient.NodeInfo{{Host: "fst.example", Port: "1095", Status: "online", CfgStatus: "on", Nofs: "1", Geotag: "test"}},
			Filesystems: []*eosclient.FSInfo{{Host: "fst.example", Port: "1095", Id: "1", StatActive: "online", StatBoot: "booted", Configstatus: "rw", Drainstatus: "prepare", StatStatfsCapacity: "1000000", StatStatfsUsedbytes: "250000"}},
		}, nil
	}
	o.shaping, o.policy, o.config = emptyOrbitCollector{}, emptyOrbitCollector{}, emptyOrbitCollector{}
	return o
}

func gatherOrbit(t *testing.T, o *OrbitCollector) map[string]*dto.MetricFamily {
	t.Helper()
	r := prometheus.NewRegistry()
	r.MustRegister(o)
	families, err := r.Gather()
	if err != nil {
		t.Fatal(err)
	}
	result := map[string]*dto.MetricFamily{}
	for _, f := range families {
		result[f.GetName()] = f
	}
	return result
}

func TestOrbitFleetKeepsNodeAndFilesystemStatesSeparate(t *testing.T) {
	o := orbitTestCollector(t)
	f := gatherOrbit(t, o)
	node := findMetric(t, f, "eos_fst_node_status_info", map[string]string{"node_id": "fst.example:1095", "active_status": "online", "config_status": "on"})
	if node.GetGauge().GetValue() != 1 {
		t.Fatal(node)
	}
	findMetric(t, f, "eos_fst_filesystem_status_info", map[string]string{"config_status": "rw", "drain_status": "prepare", "boot_status": "booted"})
	if findMetric(t, f, "eos_fst_filesystem_capacity_bytes", nil).GetGauge().GetValue() != 1000000 {
		t.Fatal("capacity")
	}
	if findMetric(t, f, "eos_fst_status_snapshot_timestamp_seconds", nil).GetGauge().GetValue() != 1700000000 {
		t.Fatal("timestamp refreshed")
	}
	o.fleet.Nodes[0].Status = ""
	o.fleet.Filesystems[0].StatStatfsCapacity = "NaN"
	f = gatherOrbit(t, o)
	findMetric(t, f, "eos_fst_node_status_info", map[string]string{"active_status": "<unknown>"})
	if f["eos_fst_filesystem_capacity_bytes"] != nil {
		t.Fatal("invalid capacity exported")
	}
}

func TestOrbitFollowerAndRoleFailureNeverExposeLeaderData(t *testing.T) {
	o := orbitTestCollector(t)
	gatherOrbit(t, o)
	o.roleAt = time.Time{}
	o.fetchRole = func(context.Context) (*eosclient.OrbitMGMRole, error) { return &eosclient.OrbitMGMRole{}, nil }
	o.fetchFleet = func(context.Context) (*eosclient.OrbitFleet, error) {
		t.Fatal("follower queried fleet")
		return nil, nil
	}
	f := gatherOrbit(t, o)
	if findMetric(t, f, "eos_mgm_master", nil).GetGauge().GetValue() != 0 {
		t.Fatal("follower called leader")
	}
	if f["eos_fst_node_status_info"] != nil || f["eos_mgm_info"] != nil || o.fleet != nil {
		t.Fatal("retained leader data")
	}
	o.roleAt = time.Time{}
	o.fetchRole = func(context.Context) (*eosclient.OrbitMGMRole, error) { return nil, errors.New("failed") }
	f = gatherOrbit(t, o)
	if f["eos_mgm_master"] != nil || findMetric(t, f, "eos_orbit_scrape_success", map[string]string{"source": "role"}).GetGauge().GetValue() != 0 {
		t.Fatal("failed role marked healthy")
	}
}

func TestOrbitFleetFailureAndRemovalDoNotKeepOldResources(t *testing.T) {
	o := orbitTestCollector(t)
	gatherOrbit(t, o)
	o.fleetAt = time.Time{}
	o.fetchFleet = func(context.Context) (*eosclient.OrbitFleet, error) { return nil, errors.New("failed") }
	f := gatherOrbit(t, o)
	if f["eos_fst_node_status_info"] != nil || f["eos_fst_status_snapshot_timestamp_seconds"] != nil {
		t.Fatal("failed refresh kept data")
	}
	o.fleetAt = time.Time{}
	o.fetchFleet = func(context.Context) (*eosclient.OrbitFleet, error) {
		return &eosclient.OrbitFleet{CollectedAt: time.Now()}, nil
	}
	f = gatherOrbit(t, o)
	if f["eos_fst_node_status_info"] != nil {
		t.Fatal("removed node retained")
	}
	if findMetric(t, f, "eos_orbit_scrape_success", map[string]string{"source": "fleet"}).GetGauge().GetValue() != 1 {
		t.Fatal("recovery failed")
	}
}

func TestOrbitConcurrentScrapesShareRoleAndFleetRefresh(t *testing.T) {
	o := orbitTestCollector(t)
	roles, fleets := 0, 0
	roleFetch, fleetFetch := o.fetchRole, o.fetchFleet
	o.fetchRole = func(ctx context.Context) (*eosclient.OrbitMGMRole, error) { roles++; return roleFetch(ctx) }
	o.fetchFleet = func(ctx context.Context) (*eosclient.OrbitFleet, error) { fleets++; return fleetFetch(ctx) }
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); gatherOrbit(t, o) }()
	}
	wg.Wait()
	if roles != 1 || fleets != 1 {
		t.Fatalf("role %d fleet %d", roles, fleets)
	}
}

func TestOrbitCounterContract(t *testing.T) {
	o := orbitTestCollector(t)
	io := counterCollector(&eosclient.IOShapingCounters{Version: 1, LimitEntries: 50000, Entries: []eosclient.IOShapingCounter{{NodeID: "/eos/fst.example:1095/fst", App: "analysis", UID: 1, GID: 2, BytesReadTotal: 1000, BytesWrittenTotal: 500, ReadOpsTotal: 10, WriteOpsTotal: 5}}})
	// Keep cluster labels identical across the actual producer collectors.
	o = NewOrbitCollector(&CollectorOpts{Cluster: io.Cluster})
	template := orbitTestCollector(t)
	o.fetchRole, o.fetchFleet = template.fetchRole, template.fetchFleet
	o.shaping = io
	o.policy = emptyOrbitCollector{}
	config := o.config.(*IOShapingConfigCollector)
	config.fetch = func(context.Context) (*eosclient.IOShapingConfig, error) {
		return &eosclient.IOShapingConfig{Enabled: true}, nil
	}
	for frame := 0; frame < 2; frame++ {
		f := gatherOrbit(t, o)
		findMetric(t, f, "eos_io_shaping_all_bytes_total", map[string]string{"node_id": "fst.example:1095", "fsid": "0", "app": "analysis"})
		if dir := os.Getenv("EOS_ORBIT_FIXTURE_DIR"); dir != "" {
			name := "before.prom"
			if frame == 1 {
				name = "after.prom"
			}
			out, err := os.Create(filepath.Join(dir, name))
			if err != nil {
				t.Fatal(err)
			}
			for _, family := range f {
				if _, err = expfmt.MetricFamilyToText(out, family); err != nil {
					t.Fatal(err)
				}
			}
			out.Close()
		}
		// Independent snapshots are cumulative; the exporter never integrates rates.
		io.snapshot.Entries[0].BytesReadTotal += 10000
		io.snapshot.Entries[0].BytesWrittenTotal += 5000
	}
}

func TestOrbitConfigFailureDoesNotReuseOldEnabledState(t *testing.T) {
	c := NewIOShapingConfigCollector(&CollectorOpts{Cluster: "test"})
	c.fetch = func(context.Context) (*eosclient.IOShapingConfig, error) {
		return &eosclient.IOShapingConfig{Enabled: true}, nil
	}
	if config, err := c.configForScrape(); err != nil || !config.Enabled {
		t.Fatal(config, err)
	}
	c.lastRefresh = time.Time{}
	c.fetch = func(context.Context) (*eosclient.IOShapingConfig, error) { return nil, errors.New("failed") }
	if config, err := c.configForScrape(); err == nil || config != nil || c.config != nil {
		t.Fatal("old config reused", config, err)
	}
}

func TestOrbitUnsupportedCountersDoNotAdvertiseCompatibleIdleCluster(t *testing.T) {
	o := orbitTestCollector(t)
	io := NewIOShapingCollector(&CollectorOpts{Cluster: "test"})
	io.fetch = func(context.Context) (*eosclient.IOShapingCounters, error) {
		return nil, errors.New("unsupported counters")
	}
	o.shaping = io
	o.fetchFleet = func(context.Context) (*eosclient.OrbitFleet, error) {
		t.Fatal("unsupported source queried fleet")
		return nil, nil
	}
	f := gatherOrbit(t, o)
	if f["eos_fst_node_status_info"] != nil || f["eos_io_shaping_config_enabled"] != nil {
		t.Fatal("unsupported source advertised compatibility")
	}
	if findMetric(t, f, "eos_io_shaping_scrape_success", nil).GetGauge().GetValue() != 0 {
		t.Fatal("failure omitted")
	}
}
