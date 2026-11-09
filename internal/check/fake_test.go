package check_test

import (
	"context"
	"fmt"
	"maps"
	"net/url"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/kokuwaio/victoriametrics-rebalance/internal/cluster"
)

// fakeCluster is a cluster.Client backed by fixed data. Pod responses are served in order per pod and path; the
// last one repeats.
type fakeCluster struct {
	info    cluster.Info
	pods    map[cluster.Component][]corev1.Pod
	podResp map[string][]string // "<pod><path>" → successive bodies
	podGets []string            // "<pod><path>" of every PodGet
	// selectGet answers vmselect API calls.
	selectGet func(path string, q url.Values) (string, error)
	pdbs      []policyv1.PodDisruptionBudget
}

func (f *fakeCluster) Info() cluster.Info { return f.info }

func (f *fakeCluster) Pods(_ context.Context, c cluster.Component) ([]corev1.Pod, error) {
	return f.pods[c], nil
}

func (f *fakeCluster) PodGet(_ context.Context, pod *corev1.Pod, _ cluster.Component, path string) ([]byte, error) {
	key := pod.Name + path
	f.podGets = append(f.podGets, key)
	resp, ok := f.podResp[key]
	if !ok || len(resp) == 0 {
		if path == "/internal/resetRollupResultCache" {
			return nil, nil
		}
		return nil, fmt.Errorf("no response for %s", key)
	}
	body := resp[0]
	if len(resp) > 1 {
		f.podResp[key] = resp[1:]
	}
	return []byte(body), nil
}

func (f *fakeCluster) SelectGet(_ context.Context, path string, q url.Values) ([]byte, error) {
	body, err := f.selectGet(path, q)
	return []byte(body), err
}

func (f *fakeCluster) PDBs(context.Context) ([]policyv1.PodDisruptionBudget, error) {
	return f.pdbs, nil
}

func newCluster(name string, storageReplicas int32) *fakeCluster {
	f := &fakeCluster{
		info: cluster.Info{
			Name:              name,
			Namespace:         "monitoring",
			Status:            "operational",
			Operational:       true,
			Retention:         "30d",
			ReplicationFactor: 1,
			StorageReplicas:   storageReplicas,
			Args:              map[cluster.Component]map[string]string{},
		},
		pods:    map[cluster.Component][]corev1.Pod{},
		podResp: map[string][]string{},
	}
	for _, c := range cluster.Components {
		n := int32(1)
		if c == cluster.Storage {
			n = storageReplicas
		}
		for i := range n {
			f.addPod(c, fmt.Sprintf("%s-%s-%d", c, name, i), "v1.150.0", nil)
		}
	}
	return f
}

func (f *fakeCluster) addPod(c cluster.Component, name, tag string, extraLabels map[string]string) *corev1.Pod {
	l := map[string]string{"app.kubernetes.io/name": string(c), "app.kubernetes.io/instance": f.info.Name}
	maps.Copy(l, extraLabels)
	f.pods[c] = append(f.pods[c], corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "monitoring", Labels: l},
		Spec: corev1.PodSpec{Containers: []corev1.Container{
			{Name: string(c), Image: "victoriametrics/" + string(c) + ":" + tag + "-cluster"},
		}},
	})
	return &f.pods[c][len(f.pods[c])-1]
}

// setDisk makes every vmstorage pod report used bytes of data and free bytes of disk space.
func (f *fakeCluster) setDisk(used, free int) {
	for _, p := range f.pods[cluster.Storage] {
		f.podResp[p.Name+"/metrics"] = []string{fmt.Sprintf(
			"# HELP vm_data_size_bytes\nvm_data_size_bytes{type=\"storage/big\"} %d\nvm_data_size_bytes{type=\"indexdb/file\"} %d\n"+
				"vm_free_disk_space_bytes{path=\"/vm-data\"} %d\n",
			used/2, used-used/2, free)}
	}
}

// setInserted makes the vminsert pods report successive vm_rows_inserted_total counters: live rows (remote write)
// and native rows (imports, as the tool's own sync does). The pods never restart.
func (f *fakeCluster) setInserted(live, native []int) {
	f.setInsertedWithStarts(live, native, make([]int, len(live)))
}

// setInsertedWithStarts is setInserted with a vm_app_start_timestamp per scrape.
func (f *fakeCluster) setInsertedWithStarts(live, native, started []int) {
	for _, p := range f.pods[cluster.Insert] {
		var bodies []string
		for i := range live {
			bodies = append(bodies, fmt.Sprintf(
				"vm_rows_inserted_total{type=\"promremotewrite\"} %d\nvm_rows_inserted_total{type=\"native\"} %d\n"+
					"vm_app_start_timestamp %d\n",
				live[i], native[i], started[i]))
		}
		f.podResp[p.Name+"/metrics"] = bodies
	}
}

func pdb(name string, match map[string]string) policyv1.PodDisruptionBudget {
	return policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "monitoring"},
		Spec:       policyv1.PodDisruptionBudgetSpec{Selector: &metav1.LabelSelector{MatchLabels: match}},
	}
}
