package shard

import (
	"hash/fnv"
	"sort"
)

// NumShards defines the fixed number of logical shards (buckets) across the keyspace.
const NumShards = 10

// Key2Shard deterministically maps any user key to a shard ID in [0, NumShards).
func Key2Shard(key string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() % NumShards)
}

// Config is an immutable, versioned cluster topology snapshot managed by the Shard Controller.
type Config struct {
	Num    int             // Monotonically increasing config version (0, 1, 2, ...)
	Shards [NumShards]int  // shardID -> owning Raft GroupID (0 = unassigned)
	Groups map[int][]int   // groupID -> list of replica NodeIDs
}

func (c Config) Clone() Config {
	groupsCopy := make(map[int][]int, len(c.Groups))
	for gid, nodes := range c.Groups {
		cp := make([]int, len(nodes))
		copy(cp, nodes)
		groupsCopy[gid] = cp
	}
	return Config{
		Num:    c.Num,
		Shards: c.Shards,
		Groups: groupsCopy,
	}
}

// Controller is the cluster control-plane state machine (analogous to Raft Group 0).
// It maintains a linearizable history of Config versions and rebalances shards
// with minimal data movement whenever replica groups join or leave.
type Controller struct {
	configs []Config
}

// NewController initializes a Shard Controller at Config #0 (no groups, all shards unassigned).
func NewController() *Controller {
	return &Controller{
		configs: []Config{
			{
				Num:    0,
				Groups: make(map[int][]int),
			},
		},
	}
}

// Join adds new replica groups and produces a balanced Config(N+1) with minimal shard moves.
func (c *Controller) Join(newGroups map[int][]int) Config {
	next := c.configs[len(c.configs)-1].Clone()
	next.Num++

	for gid, nodes := range newGroups {
		cp := make([]int, len(nodes))
		copy(cp, nodes)
		next.Groups[gid] = cp
	}

	rebalance(&next)
	c.configs = append(c.configs, next)
	return next.Clone()
}

// Leave removes replica groups and redistributes their shards evenly across remaining groups.
func (c *Controller) Leave(gids []int) Config {
	next := c.configs[len(c.configs)-1].Clone()
	next.Num++

	leaving := make(map[int]bool, len(gids))
	for _, gid := range gids {
		delete(next.Groups, gid)
		leaving[gid] = true
	}

	for s := 0; s < NumShards; s++ {
		if leaving[next.Shards[s]] {
			next.Shards[s] = 0
		}
	}

	rebalance(&next)
	c.configs = append(c.configs, next)
	return next.Clone()
}

// Move explicitly assigns a single shard to target gid (useful for hot-spot mitigation).
func (c *Controller) Move(shardID, gid int) Config {
	next := c.configs[len(c.configs)-1].Clone()
	next.Num++
	if shardID >= 0 && shardID < NumShards {
		if _, exists := next.Groups[gid]; exists {
			next.Shards[shardID] = gid
		}
	}
	c.configs = append(c.configs, next)
	return next.Clone()
}

// Query returns Config version num (or the latest Config if num < 0 or >= len).
func (c *Controller) Query(num int) Config {
	if num < 0 || num >= len(c.configs) {
		return c.configs[len(c.configs)-1].Clone()
	}
	return c.configs[num].Clone()
}

// rebalance redistributes shards deterministically across sorted GroupIDs with minimal movement.
func rebalance(cfg *Config) {
	if len(cfg.Groups) == 0 {
		for s := 0; s < NumShards; s++ {
			cfg.Shards[s] = 0
		}
		return
	}

	gids := make([]int, 0, len(cfg.Groups))
	for gid := range cfg.Groups {
		gids = append(gids, gid)
	}
	sort.Ints(gids)

	base := NumShards / len(gids)
	rem := NumShards % len(gids)
	targetCount := make(map[int]int, len(gids))
	for i, gid := range gids {
		targetCount[gid] = base
		if i < rem {
			targetCount[gid]++
		}
	}

	// Count currently assigned shards per group and collect unassigned/excess shards.
	currentCount := make(map[int]int, len(gids))
	var pool []int
	for s := 0; s < NumShards; s++ {
		owner := cfg.Shards[s]
		if _, ok := targetCount[owner]; !ok || currentCount[owner] >= targetCount[owner] {
			pool = append(pool, s)
			cfg.Shards[s] = 0
		} else {
			currentCount[owner]++
		}
	}

	// Assign pooled shards to under-loaded groups in deterministic order.
	poolIdx := 0
	for _, gid := range gids {
		for currentCount[gid] < targetCount[gid] && poolIdx < len(pool) {
			cfg.Shards[pool[poolIdx]] = gid
			currentCount[gid]++
			poolIdx++
		}
	}
}
