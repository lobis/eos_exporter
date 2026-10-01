package collector

import (
	"context"
	"log"
	"math"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/cern-eos/eos_exporter/eosclient"
	"github.com/prometheus/client_golang/prometheus"
)

const orbitRoleTTL = time.Second
const orbitFleetTTL = 30 * time.Second

// The opt-in EOS 5.4 endpoint carries the same vocabulary as the native MGM
// exporter. A measured local role gates all cluster counters and fleet data.
type OrbitCollector struct {
	mu                      sync.Mutex
	role                    *eosclient.OrbitMGMRole
	fleet                   *eosclient.OrbitFleet
	roleAt, fleetAt         time.Time
	roleErr, fleetErr       error
	fetchRole               func(context.Context) (*eosclient.OrbitMGMRole, error)
	fetchFleet              func(context.Context) (*eosclient.OrbitFleet, error)
	shaping, policy, config prometheus.Collector
	desc                    map[string]*prometheus.Desc
}

func NewOrbitCollector(opts *CollectorOpts) *OrbitCollector {
	client, _ := eosclient.New(&eosclient.Options{URL: "root://localhost", Timeout: opts.Timeout})
	shaping := NewIOShapingCollector(opts)
	shaping.fetch = client.ListIOShapingCounters
	policy := NewIOShapingPolicyCollector(opts)
	policy.fetch = client.ListIOShapingPolicies
	config := NewIOShapingConfigCollector(opts)
	config.fetch = client.ListIOShapingConfig
	o := &OrbitCollector{
		fetchRole: client.GetOrbitMGMRole, fetchFleet: client.GetOrbitFleet,
		shaping: shaping, policy: policy,
		config: config, desc: map[string]*prometheus.Desc{},
	}
	add := func(name, help string, labels ...string) {
		o.desc[name] = prometheus.NewDesc(name, help, labels, prometheus.Labels{"cluster": opts.Cluster})
	}
	add("eos_mgm_master", "Measured local MGM role (1 for leader, 0 for follower).", "mgm_id")
	add("eos_mgm_info", "Measured leader MGM build.", "mgm_id", "eos_version", "eos_release")
	add("eos_fst_node_status_info", "Original MGM node state dimensions.", "node_id", "active_status", "config_status", "geotag")
	add("eos_fst_node_filesystems", "MGM-reported filesystem count.", "node_id")
	add("eos_fst_filesystem_status_info", "Original MGM filesystem state dimensions.", "node_id", "fsid", "active_status", "boot_status", "config_status", "drain_status")
	add("eos_fst_filesystem_capacity_bytes", "MGM-reported filesystem capacity.", "node_id", "fsid")
	add("eos_fst_filesystem_used_bytes", "MGM-reported filesystem used bytes.", "node_id", "fsid")
	add("eos_fst_status_snapshot_timestamp_seconds", "Time the fleet snapshot was collected from MGM.")
	add("eos_orbit_scrape_success", "Whether a source was read successfully.", "source")
	return o
}

func (o *OrbitCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range o.desc {
		ch <- d
	}
	for _, c := range []prometheus.Collector{o.shaping, o.policy, o.config} {
		c.Describe(ch)
	}
}

func (o *OrbitCollector) Collect(ch chan<- prometheus.Metric) {
	o.mu.Lock()
	defer o.mu.Unlock()
	emit := func(name string, value float64, labels ...string) {
		ch <- prometheus.MustNewConstMetric(o.desc[name], prometheus.GaugeValue, value, labels...)
	}
	if o.roleAt.IsZero() || time.Since(o.roleAt) >= orbitRoleTTL {
		role, err := o.fetchRole(context.Background())
		o.roleAt = time.Now()
		if err != nil && o.roleErr == nil {
			log.Printf("orbit role: %v", err)
		}
		o.role, o.roleErr = role, err
		if err != nil || role == nil || !role.Leader {
			o.fleet, o.fleetAt = nil, time.Time{}
		}
	}
	if o.roleErr != nil || o.role == nil {
		emit("eos_orbit_scrape_success", 0, "role")
		return
	}
	emit("eos_orbit_scrape_success", 1, "role")
	role := 0.0
	if o.role.Leader {
		role = 1
	}
	emit("eos_mgm_master", role, o.role.MGMID)
	if !o.role.Leader {
		return
	}
	emit("eos_mgm_info", 1, o.role.MGMID, o.role.Version, o.role.Release)
	o.shaping.Collect(ch)
	// Discovery requires config plus fleet. A missing counter contract must not
	// advertise a compatible but apparently idle cluster to Orbit.
	if shaping, ok := o.shaping.(*IOShapingCollector); ok && !shaping.hasSnapshot() {
		return
	}
	if o.fleetAt.IsZero() || time.Since(o.fleetAt) >= orbitFleetTTL {
		fleet, err := o.fetchFleet(context.Background())
		o.fleetAt = time.Now()
		if err != nil && o.fleetErr == nil {
			log.Printf("orbit fleet: %v", err)
		}
		// Do not stamp cached data with a fresh time after a failed refresh.
		o.fleet, o.fleetErr = fleet, err
	}
	if o.fleetErr != nil || o.fleet == nil {
		emit("eos_orbit_scrape_success", 0, "fleet")
	} else {
		emit("eos_orbit_scrape_success", 1, "fleet")
		emit("eos_fst_status_snapshot_timestamp_seconds", float64(o.fleet.CollectedAt.Unix()))
		for _, n := range o.fleet.Nodes {
			id := net.JoinHostPort(n.Host, n.Port)
			emit("eos_fst_node_status_info", 1, id, orbitStatus(n.Status), orbitStatus(n.CfgStatus), n.Geotag)
			if v, err := strconv.ParseUint(n.Nofs, 10, 64); err == nil {
				emit("eos_fst_node_filesystems", float64(v), id)
			}
		}
		for _, f := range o.fleet.Filesystems {
			id := net.JoinHostPort(f.Host, f.Port)
			emit("eos_fst_filesystem_status_info", 1, id, f.Id, orbitStatus(f.StatActive), orbitStatus(f.StatBoot), orbitStatus(f.Configstatus), orbitStatus(f.Drainstatus))
			for _, field := range []struct{ name, value string }{
				{"eos_fst_filesystem_capacity_bytes", f.StatStatfsCapacity},
				{"eos_fst_filesystem_used_bytes", f.StatStatfsUsedbytes},
			} {
				v, err := strconv.ParseFloat(field.value, 64)
				if err == nil && v >= 0 && !math.IsInf(v, 0) && !math.IsNaN(v) {
					emit(field.name, v, id, f.Id)
				}
			}
		}
	}
	for _, c := range []prometheus.Collector{o.config, o.policy} {
		c.Collect(ch)
	}
}

func orbitStatus(status string) string {
	if status == "" {
		return "<unknown>"
	}
	return status
}
