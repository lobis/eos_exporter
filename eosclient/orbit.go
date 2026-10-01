package eosclient

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"
)

type OrbitMGMRole struct {
	Leader                  bool
	MGMID, Version, Release string
}

type OrbitFleet struct {
	Nodes       []*NodeInfo
	Filesystems []*FSInfo
	CollectedAt time.Time
}

func (c *Client) orbitCommand(ctx context.Context, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, c.opt.EosBinary, args...)
	// Role evidence must come from this MGM, not an instance alias that redirects.
	cmd.Env = append(os.Environ(), "EOS_MGM_URL="+c.opt.URL)
	out, stderr, err := c.execute(cmd)
	if err != nil {
		return "", fmt.Errorf("orbit %v: %w (%s)", args, err, strings.TrimSpace(stderr))
	}
	return out, nil
}

func (c *Client) GetOrbitMGMRole(ctx context.Context) (*OrbitMGMRole, error) {
	ctx, cancel := c.getTimeout(ctx)
	defer cancel()
	out, err := c.orbitCommand(ctx, "ns", "stat", "-m")
	if err != nil {
		return nil, err
	}
	leader, id, err := parseLocalMGMRole(out)
	if err != nil {
		return nil, err
	}
	role := &OrbitMGMRole{Leader: leader, MGMID: id}
	// A follower's version command may be redirected; do not label it as local.
	if !leader {
		return role, nil
	}
	out, err = c.orbitCommand(ctx, "version")
	if err != nil {
		return nil, err
	}
	role.Version, role.Release, err = parseMGMVersion(out)
	if err != nil {
		return nil, err
	}
	return role, nil
}

func (c *Client) GetOrbitFleet(ctx context.Context) (*OrbitFleet, error) {
	ctx, cancel := c.getTimeout(ctx)
	defer cancel()
	nodes, err := c.orbitCommand(ctx, "node", "ls", "-m")
	if err != nil {
		return nil, err
	}
	filesystems, err := c.orbitCommand(ctx, "fs", "ls", "-m")
	if err != nil {
		return nil, err
	}
	fleet := &OrbitFleet{CollectedAt: time.Now()}
	// Unlike the legacy collectors, an invalid row fails the snapshot. Silently
	// dropping a row would turn a parse failure into a removed node or disk.
	seenNodes, seenFS := map[string]bool{}, map[string]bool{}
	for _, row := range strings.Split(strings.TrimSpace(nodes), "\n") {
		if strings.TrimSpace(row) == "" {
			continue
		}
		node, err := c.parseNodeInfo(row)
		if err != nil {
			return nil, err
		}
		key := node.Host + ":" + node.Port
		if seenNodes[key] {
			return nil, fmt.Errorf("duplicate orbit node %s", key)
		}
		seenNodes[key] = true
		fleet.Nodes = append(fleet.Nodes, node)
	}
	for _, row := range strings.Split(strings.TrimSpace(filesystems), "\n") {
		if strings.TrimSpace(row) == "" {
			continue
		}
		fs, err := c.parseFSInfo(row)
		if err != nil {
			return nil, err
		}
		if fs.Host == "" || fs.Port == "" || fs.Id == "" || fs.Id == "0" {
			return nil, fmt.Errorf("invalid orbit filesystem identity")
		}
		if seenFS[fs.Id] {
			return nil, fmt.Errorf("duplicate orbit filesystem %s", fs.Id)
		}
		seenFS[fs.Id] = true
		// EOS 5.4 calls this dimension local.drain in monitoring output.
		if drain, present := c.getMap(row)["local.drain"]; present {
			fs.Drainstatus = drain
		}
		if !seenNodes[fs.Host+":"+fs.Port] {
			return nil, fmt.Errorf("orbit filesystem %s belongs to an unlisted node", fs.Id)
		}
		fleet.Filesystems = append(fleet.Filesystems, fs)
	}
	return fleet, nil
}
