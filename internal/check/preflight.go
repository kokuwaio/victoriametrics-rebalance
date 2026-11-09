package check

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"

	"github.com/kokuwaio/victoriametrics-rebalance/internal/cluster"
)

// Names of the preflight checks.
const (
	Operational       = "operational"
	StorageNodes      = "storage nodes"
	Version           = "version"
	Retention         = "retention"
	ReplicationFactor = "replication factor"
	Dedup             = "deduplication"
	Capacity          = "capacity"
	DualWrite         = "dual write"
	PodLabels         = "pod labels"
)

const dedupFlag = "dedup.minScrapeInterval"

// PreflightOptions tune the preflight checks.
type PreflightOptions struct {
	// Headroom is the share of free space green needs on top of blue's used bytes (0.2 = 20%).
	Headroom float64
	// RateWindow is the time between the two scrapes that measure ingestion rates.
	RateWindow time.Duration
	Sleep      Sleeper
}

// Preflight checks that green is ready to receive blue's data.
func Preflight(ctx context.Context, blue, green cluster.Client, o PreflightOptions) Report {
	var r Report
	blueInfo, greenInfo := blue.Info(), green.Info()

	operational(&r, blueInfo, greenInfo)
	if greenInfo.StorageReplicas > blueInfo.StorageReplicas {
		r.add(StorageNodes, Pass, "blue has %d vmstorage nodes, green %d", blueInfo.StorageReplicas, greenInfo.StorageReplicas)
	} else {
		r.add(StorageNodes, Warn, "green has %d vmstorage nodes, blue %d: nothing to rebalance", greenInfo.StorageReplicas, blueInfo.StorageReplicas)
	}
	versions(ctx, &r, blue, green)
	if blueInfo.Retention == greenInfo.Retention {
		r.add(Retention, Pass, "both %q", blueInfo.Retention)
	} else {
		r.add(Retention, Fail, "blue %q, green %q", blueInfo.Retention, greenInfo.Retention)
	}
	if blueInfo.ReplicationFactor == greenInfo.ReplicationFactor {
		r.add(ReplicationFactor, Pass, "both %d", blueInfo.ReplicationFactor)
	} else {
		r.add(ReplicationFactor, Fail, "blue %d, green %d", blueInfo.ReplicationFactor, greenInfo.ReplicationFactor)
	}
	dedup(&r, greenInfo)
	capacity(ctx, &r, blue, green, o.Headroom)
	dualWrite(ctx, &r, blue, green, o)
	podLabels(ctx, &r, blue, green)
	return r
}

func operational(r *Report, b, g cluster.Info) {
	var bad []string
	for _, i := range []cluster.Info{b, g} {
		if !i.Operational {
			bad = append(bad, fmt.Sprintf("%s is %q", i.Name, i.Status))
		}
	}
	if len(bad) > 0 {
		r.add(Operational, Fail, "%s", strings.Join(bad, ", "))
		return
	}
	r.add(Operational, Pass, "both operational")
}

// versions compares the image tags of the running pods per component. The native export format is
// version-coupled, so blue and green must run the same version.
func versions(ctx context.Context, r *Report, blue, green cluster.Client) {
	var diffs []string
	for _, c := range cluster.Components {
		bt, err := imageTags(ctx, blue, c)
		if err != nil {
			r.add(Version, Fail, "%v", err)
			return
		}
		gt, err := imageTags(ctx, green, c)
		if err != nil {
			r.add(Version, Fail, "%v", err)
			return
		}
		if len(bt) != 1 || !slices.Equal(bt, gt) {
			diffs = append(diffs, fmt.Sprintf("%s: blue %s, green %s", c, strings.Join(bt, ", "), strings.Join(gt, ", ")))
		}
	}
	if len(diffs) > 0 {
		r.add(Version, Fail, "%s", strings.Join(diffs, "; "))
		return
	}
	r.add(Version, Pass, "same image tags on all components")
}

func imageTags(ctx context.Context, c cluster.Client, comp cluster.Component) ([]string, error) {
	pods, err := cluster.RunningPods(ctx, c, comp)
	if err != nil {
		return nil, err
	}
	var tags []string
	for _, p := range pods {
		tag := imageTag(containerImage(p, string(comp)))
		if !slices.Contains(tags, tag) {
			tags = append(tags, tag)
		}
	}
	slices.Sort(tags)
	return tags, nil
}

// imageTag returns the tag of an image reference, or its digest if it has one. A registry port
// (registry:5000/vmstorage) is not mistaken for a tag.
func imageTag(image string) string {
	if i := strings.LastIndex(image, "@"); i >= 0 {
		return image[i+1:]
	}
	name := image[strings.LastIndex(image, "/")+1:]
	if i := strings.LastIndex(name, ":"); i >= 0 {
		return name[i+1:]
	}
	return "latest"
}

func containerImage(p corev1.Pod, name string) string {
	for _, c := range p.Spec.Containers {
		if c.Name == name {
			return c.Image
		}
	}
	if len(p.Spec.Containers) > 0 {
		return p.Spec.Containers[0].Image
	}
	return ""
}

// dedup checks that green drops samples with equal timestamps, which makes overlapping sync windows safe.
// VictoriaMetrics cluster needs the flag on vmselect and vmstorage.
func dedup(r *Report, g cluster.Info) {
	var missing []string
	for _, c := range []cluster.Component{cluster.Storage, cluster.Select} {
		if g.Args[c][dedupFlag] == "" {
			missing = append(missing, string(c))
		}
	}
	if len(missing) > 0 {
		r.add(Dedup, Fail, "green has no -%s on %s; set it to 1ms", dedupFlag, strings.Join(missing, " and "))
		return
	}
	st, sel := g.Args[cluster.Storage][dedupFlag], g.Args[cluster.Select][dedupFlag]
	if st != "1ms" || sel != "1ms" {
		r.add(Dedup, Warn, "green: -%s=%s on vmstorage, %s on vmselect; anything above 1ms also merges real samples",
			dedupFlag, st, sel)
		return
	}
	r.add(Dedup, Pass, "green: -%s=1ms", dedupFlag)
}

// capacity compares blue's used bytes (vm_data_size_bytes) with green's free disk space (vm_free_disk_space_bytes),
// both scraped from every vmstorage pod.
func capacity(ctx context.Context, r *Report, blue, green cluster.Client, headroom float64) {
	used, err := scrape(ctx, blue, cluster.Storage, "vm_data_size_bytes", nil)
	if err != nil {
		r.add(Capacity, Fail, "%v", err)
		return
	}
	free, err := scrape(ctx, green, cluster.Storage, "vm_free_disk_space_bytes", nil)
	if err != nil {
		r.add(Capacity, Fail, "%v", err)
		return
	}
	u, f := total(used), total(free)
	need := u * (1 + headroom)
	if f < need {
		r.add(Capacity, Fail, "green has %s free, blue uses %s; green needs %s with %.0f%% headroom",
			FormatBytes(f), FormatBytes(u), FormatBytes(need), headroom*100)
		return
	}
	r.add(Capacity, Pass, "green has %s free, blue uses %s", FormatBytes(f), FormatBytes(u))
}

// dualWrite checks that green receives live writes. Rows imported in native format are left out on green, because
// that's what the tool's own sync sends.
func dualWrite(ctx context.Context, r *Report, blue, green cluster.Client, o PreflightOptions) {
	// Measure both clusters over the same window, so the share is comparable.
	type rate struct {
		v   float64
		err error
	}
	done := make(chan rate, 1)
	go func() {
		v, err := insertRate(ctx, blue, o.RateWindow, o.Sleep, nil)
		done <- rate{v, err}
	}()
	gr, gerr := insertRate(ctx, green, o.RateWindow, o.Sleep, func(l map[string]string) bool { return l["type"] != "native" })
	b := <-done
	br := b.v
	if err := firstErr(b.err, gerr); err != nil {
		r.add(DualWrite, Fail, "%v", err)
		return
	}
	if gr == 0 {
		r.add(DualWrite, Fail, "green receives no live writes: add it as a second remoteWrite URL on the VMAgent(s)")
		return
	}
	r.DualWriteActive = true
	if br == 0 {
		r.add(DualWrite, Warn, "green receives %.0f rows/s, but blue receives none", gr)
		return
	}
	share := gr / br
	msg := fmt.Sprintf("green receives %.0f rows/s, %.0f%% of blue's %.0f rows/s", gr, share*100, br)
	if share < 0.99 {
		r.add(DualWrite, Warn, "%s; the rest bypasses dual write and relies on the final sync", msg)
		return
	}
	r.add(DualWrite, Pass, "%s", msg)
}

func firstErr(errs ...error) error {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

// podLabels finds PodDisruptionBudgets and topologySpreadConstraints whose selectors match pods of both clusters.
// Those count blue and green pods together, so green pods may not schedule, or evictions may be blocked.
func podLabels(ctx context.Context, r *Report, blue, green cluster.Client) {
	bluePods, err := allPods(ctx, blue)
	if err != nil {
		r.add(PodLabels, Fail, "%v", err)
		return
	}
	greenPods, err := allPods(ctx, green)
	if err != nil {
		r.add(PodLabels, Fail, "%v", err)
		return
	}
	var problems []string
	overlaps := func(sel *metav1.LabelSelector) bool {
		s, err := metav1.LabelSelectorAsSelector(sel)
		return err == nil && !s.Empty() && matchesAny(s, bluePods) && matchesAny(s, greenPods)
	}

	seen := map[string]bool{}
	for _, c := range []cluster.Client{blue, green} {
		pdbs, err := c.PDBs(ctx)
		if err != nil {
			r.add(PodLabels, Fail, "list PodDisruptionBudgets: %v", err)
			return
		}
		for _, p := range pdbs {
			key := p.Namespace + "/" + p.Name
			if seen[key] {
				continue
			}
			seen[key] = true
			if overlaps(p.Spec.Selector) {
				problems = append(problems, fmt.Sprintf("PodDisruptionBudget %s selects pods of both clusters", p.Name))
			}
		}
	}
	for _, p := range append(slices.Clone(bluePods), greenPods...) {
		for _, tsc := range p.Spec.TopologySpreadConstraints {
			key := "tsc/" + metav1.FormatLabelSelector(tsc.LabelSelector)
			if seen[key] {
				continue
			}
			seen[key] = true
			if overlaps(tsc.LabelSelector) {
				problems = append(problems, fmt.Sprintf("topologySpreadConstraints of pod %s (%s) select pods of both clusters",
					p.Name, metav1.FormatLabelSelector(tsc.LabelSelector)))
			}
		}
	}
	if len(problems) > 0 {
		r.add(PodLabels, Fail, "%s; select on app.kubernetes.io/instance as well", strings.Join(problems, "; "))
		return
	}
	r.add(PodLabels, Pass, "no selector matches pods of both clusters")
}

func allPods(ctx context.Context, c cluster.Client) ([]corev1.Pod, error) {
	var out []corev1.Pod
	for _, comp := range cluster.Components {
		pods, err := c.Pods(ctx, comp)
		if err != nil {
			return nil, err
		}
		out = append(out, pods...)
	}
	return out, nil
}

func matchesAny(s labels.Selector, pods []corev1.Pod) bool {
	return slices.ContainsFunc(pods, func(p corev1.Pod) bool { return s.Matches(labels.Set(p.Labels)) })
}
