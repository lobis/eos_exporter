package eosclient

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func fixtureOrbitClient(t *testing.T, nodes, filesystems string) *Client {
	t.Helper()
	dir := t.TempDir()
	for name, value := range map[string]string{"nodes": nodes, "filesystems": filesystems} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(value), 0600); err != nil {
			t.Fatal(err)
		}
	}
	script := `#!/bin/sh
[ "$EOS_MGM_URL" = root://localhost ] || exit 8
case "$*" in
 'ns stat -m') echo "is_master=${TEST_ROLE:-true} master_id=mgm.example:1094" ;;
 version) [ "$TEST_ROLE" != false ] || exit 9; echo 'EOS_SERVER_VERSION=5.4.12 EOS_SERVER_RELEASE=1' ;;
 'node ls -m') cat "$(dirname "$0")/nodes" ;;
 'fs ls -m') cat "$(dirname "$0")/filesystems" ;;
 *) exit 10 ;;
esac
`
	binary := filepath.Join(dir, "eos")
	if err := os.WriteFile(binary, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	c, err := New(&Options{EosBinary: binary, URL: "root://localhost", Timeout: 2})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestOrbitRoleUsesLocalEvidenceAndDoesNotQueryFollowerVersion(t *testing.T) {
	c := fixtureOrbitClient(t, "", "")
	role, err := c.GetOrbitMGMRole(context.Background())
	if err != nil || !role.Leader || role.MGMID != "mgm.example:1094" || role.Version != "5.4.12" {
		t.Fatalf("%+v %v", role, err)
	}
	t.Setenv("TEST_ROLE", "false")
	role, err = c.GetOrbitMGMRole(context.Background())
	if err != nil || role.Leader || role.MGMID != "" || role.Version != "" {
		t.Fatalf("%+v %v", role, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err = c.GetOrbitMGMRole(ctx); err == nil {
		t.Fatal("cancel ignored")
	}
}

func TestOrbitFleetParsesCapturedEOS54States(t *testing.T) {
	nodes, err := os.ReadFile("../testdata/orbit-eos54-node.txt")
	if err != nil {
		t.Fatal(err)
	}
	fs, err := os.ReadFile("../testdata/orbit-eos54-fs.txt")
	if err != nil {
		t.Fatal(err)
	}
	c := fixtureOrbitClient(t, string(nodes), string(fs))
	fleet, err := c.GetOrbitFleet(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(fleet.Nodes) != 2 || len(fleet.Filesystems) != 5 || fleet.CollectedAt.IsZero() {
		t.Fatalf("%+v", fleet)
	}
	if fleet.Nodes[0].Status != "online" || fleet.Nodes[0].CfgStatus != "on" || fleet.Filesystems[0].Configstatus != "ro" || fleet.Filesystems[0].Drainstatus != "nodrain" {
		t.Fatalf("raw states lost: %+v %+v", fleet.Nodes[0], fleet.Filesystems[0])
	}
	// A filesystem configured rw is not evidence that its node is online.
	if fleet.Nodes[1].Status != "unknown" || fleet.Filesystems[1].StatBoot != "" {
		t.Fatal("invented state")
	}
}

func TestOrbitFleetRejectsIncompleteOrDuplicateInventory(t *testing.T) {
	node := "hostport=fst.example:1095 status=online cfg.status=on nofs=1"
	fs := "host=fst.example port=1095 id=1 stat.boot=booted configstatus=rw stat.active=online local.drain=nodrain"
	for _, test := range []struct{ node, fs string }{
		{"invalid", fs}, {node, "invalid"}, {node + "\n" + node, fs}, {node, fs + "\n" + fs},
		{node, strings.ReplaceAll(fs, "fst.example", "other.example")},
	} {
		if _, err := fixtureOrbitClient(t, test.node, test.fs).GetOrbitFleet(context.Background()); err == nil {
			t.Fatalf("accepted %+v", test)
		}
	}
}
